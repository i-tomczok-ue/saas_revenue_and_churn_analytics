# Databricks: plik źródłowy pipeline'u (Lakeflow Spark Declarative Pipelines), warstwa GOLD.
# Dodaj go do tego samego pipeline'u co 01_bronze.py i 02_silver.py (folder transformations).
#
# Model gwiazdy dla Power BI:
#   dim_date                     kalendarz (jeden wiersz = jeden dzień)
#   dim_customer                 klienci (SCD 1, z silver_customers)
#   dim_acquisition_channel      kanały pozyskania
#   dim_plan                     plany (dane referencyjne)
#   dim_subscription             wersje subskrypcji (SCD 2: valid_from, valid_to, is_current)
#   fact_subscription_monthly    ziarno: subskrypcja x miesiąc (MRR kontraktowy, ruchy MRR, użycie, tickety, fakturowanie)
#   fact_invoice                 ziarno: faktura (tabela typu accumulating snapshot: kamienie milowe i aktualny status)
#   gold_signal_by_months_to_churn   sprawdzenie hipotezy: użycie i tickety w miesiącach przed rezygnacją
#
# Zasady:
#   * MRR jest KONTRAKTOWY: cena planu + dodatkowe miejsca, liczony z wersji subskrypcji obowiązującej
#     na koniec miesiąca (niezależnie od płatności). Płatności to osobne miary.
#   * Wersję SCD 2 wybieramy po DACIE (koniec miesiąca zawiera się w [valid_from, valid_to)),
#     a nie po fladze is_current. Wybór po is_current dałby historii MRR bieżący plan.
#   * Rabat jest osobną miarą (discount_amount), nie zmniejsza MRR.
#   * Rezygnacja jest księgowana w miesiącu PO ostatnim dniu świadczenia usługi.
#   * Ostatni miesiąc jest liczony na dzień ostatnich załadowanych danych (as_of), jeśli miesiąc jeszcze trwa.
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

FAR_FUTURE = "9999-12-31"


# ----------------------------------------------------------------------------- wymiary
@dp.materialized_view(name="dim_date", comment="Kalendarz 2025-2027.")
def dim_date():
    days = spark.sql("SELECT explode(sequence(DATE'2025-01-01', DATE'2027-12-31')) AS date")  # noqa: F821
    return days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.trunc("date", "month").alias("month_start"),
        F.last_day("date").alias("month_end"),
        (F.col("date") == F.last_day("date")).alias("is_month_end"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


@dp.materialized_view(name="dim_acquisition_channel", comment="Kanały pozyskania klientów.")
def dim_acquisition_channel():
    names = (
        spark.read.table("silver_customers")  # noqa: F821
        .select(F.expr("coalesce(nullif(trim(acquisition_channel), ''), 'Unknown')").alias("channel_name"))
        .distinct()
    )
    return names.withColumn("channel_key", F.row_number().over(Window.orderBy("channel_name"))).select(
        "channel_key", "channel_name")


@dp.materialized_view(name="dim_customer", comment="Klienci (SCD typu 1).")
def dim_customer():
    c = spark.read.table("silver_customers").withColumn(  # noqa: F821
        "channel_name", F.expr("coalesce(nullif(trim(acquisition_channel), ''), 'Unknown')"))
    ch = spark.read.table("dim_acquisition_channel")  # noqa: F821
    return c.join(ch, "channel_name", "left").select(
        F.col("customer_id").alias("customer_key"),
        "company_name", "industry", "company_size", "country", "signup_date", "channel_key", "channel_name",
    )


@dp.materialized_view(name="dim_plan", comment="Plany cenowe.")
def dim_plan():
    return spark.read.table("silver_plans").select(  # noqa: F821
        F.col("plan_id").alias("plan_key"), "plan_name", "monthly_price", "seats_included", "extra_seat_price")


@dp.materialized_view(
    name="dim_subscription",
    comment="Wersje subskrypcji (SCD typu 2). Wersja bieżąca ma is_current = true i valid_to = 9999-12-31.",
)
def dim_subscription():
    s = spark.read.table("silver_subscriptions")  # noqa: F821
    valid_from = F.to_date(F.col("__START_AT"))
    valid_to = F.coalesce(F.to_date(F.col("__END_AT")), F.lit(FAR_FUTURE).cast("date"))
    return s.select(
        F.concat_ws("|", F.col("subscription_id"), F.date_format(valid_from, "yyyyMMdd")).alias("subscription_key"),
        "subscription_id",
        F.col("customer_id").alias("customer_key"),
        F.col("plan_id").alias("plan_key"),
        "seats", "status", "churn_reason",
        valid_from.alias("valid_from"),
        valid_to.alias("valid_to"),
        F.col("__END_AT").isNull().alias("is_current"),
    )


# ----------------------------------------------------------------------------- pomocnicze do faktów
def _as_of():
    """Data ostatnich załadowanych danych (jeden wiersz, kolumna as_of)."""
    a = spark.read.table("silver_subscriptions").select(F.max(F.to_date("__START_AT")).alias("d"))  # noqa: F821
    b = spark.read.table("silver_usage_events").select(F.max(F.to_date("event_ts")).alias("d"))  # noqa: F821
    return a.unionByName(b).agg(F.max("d").alias("as_of"))


def _months():
    """Miesiące od początku danych do as_of; ref_date = koniec miesiąca albo as_of dla miesiąca w toku."""
    starts = (
        spark.read.table("dim_date")  # noqa: F821
        .where("date = month_start")
        .select("year_month", "month_start", "month_end")
    )
    return (
        starts.crossJoin(_as_of())
        .where("month_start <= as_of")
        .select("year_month", "month_start", "month_end", F.least("month_end", "as_of").alias("ref_date"))
    )


# ----------------------------------------------------------------------------- fact_subscription_monthly
@dp.materialized_view(
    name="fact_subscription_monthly",
    comment="Ziarno: subskrypcja x miesiąc. MRR kontraktowy na koniec miesiąca, ruchy MRR, użycie, tickety, fakturowanie.",
)
@dp.expect("mrr_nieujemne", "mrr >= 0")
@dp.expect("znany_plan", "plan_key IS NOT NULL AND mrr IS NOT NULL")
def fact_subscription_monthly():
    versions = spark.read.table("dim_subscription")  # noqa: F821
    plans = spark.read.table("dim_plan")  # noqa: F821

    # wersja subskrypcji obowiązująca w dniu ref_date (join po dacie, nie po is_current)
    sm = (
        _months()
        .join(versions, (F.col("ref_date") >= F.col("valid_from")) & (F.col("ref_date") < F.col("valid_to")), "inner")
        .join(plans, "plan_key", "left")
    )
    active = F.col("status") == "active"
    sm = (
        sm.withColumn("seats_eom", F.when(active, F.col("seats")).otherwise(0))
        .withColumn("base_plan_mrr", F.when(active, F.col("monthly_price")).otherwise(0.0))
        .withColumn("extra_seats_mrr",
                    F.when(active, F.greatest(F.col("seats") - F.col("seats_included"), F.lit(0))
                           * F.col("extra_seat_price")).otherwise(0.0))
        .withColumn("mrr", F.col("base_plan_mrr") + F.col("extra_seats_mrr"))
    )

    w = Window.partitionBy("subscription_id").orderBy("month_start")
    sm = sm.withColumn("prev_mrr", F.coalesce(F.lag("mrr").over(w), F.lit(0.0)))
    # wiersze po miesiącu rezygnacji (mrr = 0 i prev_mrr = 0) odpadają
    sm = sm.where("mrr > 0 OR prev_mrr > 0")

    sm = (
        sm.withColumn("new_mrr", F.when((F.col("prev_mrr") == 0) & (F.col("mrr") > 0), F.col("mrr")).otherwise(0.0))
        .withColumn("churned_mrr", F.when((F.col("prev_mrr") > 0) & (F.col("mrr") == 0), F.col("prev_mrr")).otherwise(0.0))
        .withColumn("expansion_mrr", F.when((F.col("prev_mrr") > 0) & (F.col("mrr") > F.col("prev_mrr")),
                                            F.col("mrr") - F.col("prev_mrr")).otherwise(0.0))
        .withColumn("contraction_mrr", F.when((F.col("mrr") > 0) & (F.col("mrr") < F.col("prev_mrr")),
                                              F.col("prev_mrr") - F.col("mrr")).otherwise(0.0))
    )

    # użycie produktu i tickety (po kliencie; w tym modelu jedna subskrypcja na klienta)
    usage = (
        spark.read.table("silver_usage_events")  # noqa: F821
        .groupBy(F.col("customer_id").alias("u_customer"), F.date_format("event_ts", "yyyy-MM").alias("u_ym"))
        .agg(F.countDistinct(F.when(F.col("user_id") != "unknown", F.col("user_id"))).alias("active_users"),
             F.count("*").alias("usage_events"))
    )
    tickets = (
        spark.read.table("silver_tickets")  # noqa: F821
        .groupBy(F.col("customer_id").alias("t_customer"), F.date_format("created_at", "yyyy-MM").alias("t_ym"))
        .agg(F.count("*").alias("support_tickets"),
             F.sum(F.when(F.col("priority") == "high", 1).otherwise(0)).alias("high_priority_tickets"))
    )

    # fakturowanie (miara osobna od MRR): faktury anulowane (void) pomijamy
    inv = (
        spark.read.table("silver_invoices")  # noqa: F821
        .where("status <> 'void'")
        .groupBy(F.col("subscription_id").alias("i_subscription"), F.date_format("invoice_date", "yyyy-MM").alias("i_ym"))
        .agg(
            F.count("*").alias("invoices_count"),
            F.sum(F.expr("aggregate(lines, 0D, (acc, x) -> acc + x.amount)")).alias("gross_billed_amount"),
            F.sum(F.coalesce(F.col("discount.amount"), F.lit(0.0))).alias("discount_amount"),
            F.sum("total").alias("billed_amount"),
            F.sum(F.when(F.col("status") == "paid", F.col("total")).otherwise(0.0)).alias("collected_amount"),
            F.sum(F.when(F.col("status").isin("open", "failed"), F.col("total")).otherwise(0.0)).alias("outstanding_amount"),
            F.sum(F.when(F.col("status") == "uncollectible", F.col("total")).otherwise(0.0)).alias("written_off_amount"),
            F.sum(F.when(F.expr("exists(payment_attempts, x -> x.result = 'declined')"), 1).otherwise(0))
            .alias("invoices_with_failed_payment"),
        )
    )

    f = (
        sm.join(usage, (F.col("customer_key") == F.col("u_customer")) & (F.col("year_month") == F.col("u_ym")), "left")
        .join(tickets, (F.col("customer_key") == F.col("t_customer")) & (F.col("year_month") == F.col("t_ym")), "left")
        .join(inv, (F.col("subscription_id") == F.col("i_subscription")) & (F.col("year_month") == F.col("i_ym")), "left")
        .drop("u_customer", "u_ym", "t_customer", "t_ym", "i_subscription", "i_ym")
    )

    zero = ["active_users", "usage_events", "support_tickets", "high_priority_tickets", "invoices_count",
            "gross_billed_amount", "discount_amount", "billed_amount", "collected_amount", "outstanding_amount",
            "written_off_amount", "invoices_with_failed_payment"]
    f = f.fillna(0, subset=zero)

    return f.select(
        F.date_format("month_end", "yyyyMMdd").cast("int").alias("date_key"),
        "year_month",
        "subscription_key", "subscription_id", "customer_key", "plan_key",
        F.col("status").alias("status_eom"),
        "churn_reason",
        F.col("seats_eom").alias("seats"),
        "seats_included",
        "mrr", "base_plan_mrr", "extra_seats_mrr",
        "new_mrr", "expansion_mrr", "contraction_mrr", "churned_mrr",
        "active_users", "usage_events",
        F.when(F.col("seats_eom") > 0, F.col("active_users") / F.col("seats_eom")).alias("seat_utilization"),
        "support_tickets", "high_priority_tickets",
        "invoices_count", "gross_billed_amount", "discount_amount", "billed_amount", "collected_amount",
        "outstanding_amount", "written_off_amount", "invoices_with_failed_payment",
        "month_start",
        F.col("ref_date").alias("snapshot_date"),
        # w Power BI liczby wskaźników (utylizacja, NRR) licz jako DIVIDE(SUM(licznik), SUM(mianownik))
    )


# ----------------------------------------------------------------------------- fact_invoice (accumulating snapshot)
@dp.materialized_view(
    name="fact_invoice",
    comment="Ziarno: faktura. Aktualny status i kamienie milowe (wystawiona, pierwsza nieudana płatność, zapłacona).",
)
def fact_invoice():
    inv = spark.read.table("silver_invoices")  # noqa: F821
    attempts = (
        spark.read.table("silver_payment_attempts")  # noqa: F821
        .groupBy("invoice_id")
        .agg(
            F.min(F.when(F.col("result") == "declined", F.col("attempt_at"))).alias("first_failed_at"),
            F.max_by("decline_reason", "attempt").alias("last_decline_reason"),
        )
    )
    as_of = _as_of()
    gross = F.expr("aggregate(lines, 0D, (acc, x) -> acc + x.amount)")
    f = inv.join(attempts, "invoice_id", "left").crossJoin(as_of)

    def dkey(col):
        return F.date_format(col, "yyyyMMdd").cast("int")

    paid_date = F.to_date("paid_at")
    closed_date = F.when(F.col("status") == "paid", paid_date).when(
        F.col("status").isin("uncollectible", "void"), F.to_date("updated_at"))
    return f.select(
        "invoice_id", "subscription_id",
        F.col("customer_id").alias("customer_key"),
        dkey("invoice_date").alias("invoice_date_key"),
        dkey("due_date").alias("due_date_key"),
        dkey(F.to_date("first_failed_at")).alias("first_failed_date_key"),
        dkey(paid_date).alias("paid_date_key"),
        dkey(closed_date).alias("closed_date_key"),
        "status", "payment_method", "attempts_count", "last_decline_reason",
        gross.alias("gross_amount"),
        F.coalesce(F.col("discount.amount"), F.lit(0.0)).alias("discount_amount"),
        F.col("total").alias("total_amount"),
        F.when(F.col("status") == "paid", F.col("total")).otherwise(0.0).alias("collected_amount"),
        F.when(F.col("status").isin("open", "failed"), F.col("total")).otherwise(0.0).alias("outstanding_amount"),
        F.when(F.col("status") == "uncollectible", F.col("total")).otherwise(0.0).alias("written_off_amount"),
        F.when(F.col("status") == "paid", F.datediff(paid_date, F.col("invoice_date"))).alias("days_to_pay"),
        (F.col("status").isin("open", "failed") & (F.col("due_date") < F.col("as_of"))).alias("is_overdue"),
        F.when(F.col("status").isin("open", "failed") & (F.col("due_date") < F.col("as_of")),
               F.datediff(F.col("as_of"), F.col("due_date"))).alias("days_overdue"),
        F.col("first_failed_at").isNotNull().alias("had_failed_payment"),
        F.col("updated_at").alias("last_status_at"),
    )


# ----------------------------------------------------------------------------- hipoteza: sygnały przed rezygnacją
@dp.materialized_view(
    name="gold_signal_by_months_to_churn",
    comment="Średnia utylizacja miejsc i tickety w miesiącach przed rezygnacją vs. klienci, którzy zostali.",
)
def gold_signal_by_months_to_churn():
    f = spark.read.table("fact_subscription_monthly")  # noqa: F821
    churn = (
        f.where("churned_mrr > 0")
        .select("subscription_id", F.col("month_start").alias("churn_month"), F.col("churn_reason").alias("reason"))
    )
    j = f.where("mrr > 0").join(churn, "subscription_id", "left")
    months_to_churn = F.when(F.col("churn_month").isNotNull(),
                             F.months_between("churn_month", "month_start").cast("int"))
    cohort = F.when(F.col("reason").isNotNull(), F.concat(F.lit("churned_"), F.col("reason"))).otherwise("retained")
    return (
        j.withColumn("cohort", cohort)
        .withColumn("months_to_churn", months_to_churn)
        .where("months_to_churn IS NULL OR months_to_churn BETWEEN 1 AND 12")
        .groupBy("cohort", "months_to_churn")
        .agg(
            F.countDistinct("subscription_id").alias("subscriptions"),
            F.count("*").alias("subscription_months"),
            F.round(F.sum("active_users") / F.sum("seats"), 3).alias("seat_utilization"),
            F.round(F.sum("support_tickets") / F.count("*"), 3).alias("tickets_per_subscription_month"),
            F.round(F.avg("mrr"), 1).alias("avg_mrr"),
        )
        .orderBy("cohort", F.col("months_to_churn").desc_nulls_last())
    )
