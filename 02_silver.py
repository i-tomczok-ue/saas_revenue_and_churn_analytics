# Databricks: plik źródłowy pipeline'u (Lakeflow Spark Declarative Pipelines), warstwa SILVER.
# Dodaj go do tego samego pipeline'u co 01_bronze.py (folder transformations).
#
# Zasada: najpierw WALIDACJA, potem historia (SCD) i deduplikacja.
#   * każdy wiersz z Bronze dostaje powód kwarantanny (_quarantine_reason) albo null,
#   * wiersze z powodem trafiają do jednej tabeli silver_quarantine (z oryginalnym tekstem),
#   * czyste wiersze są normalizowane i zasilają tabele Silver.
#
#   silver_customers          SCD typ 1  (zostaje aktualna nazwa firmy; korekta literówki nadpisuje)
#   silver_subscriptions      SCD typ 2  (wersje: plan, liczba miejsc, status; kolumny __START_AT, __END_AT)
#   silver_invoices           aktualny stan faktury (upsert po updated_at); pełna historia zostaje w Bronze
#   silver_invoice_lines      pozycje faktur (spłaszczone)
#   silver_payment_attempts   próby płatności (spłaszczone)
#   silver_usage_events       zdarzenia po deduplikacji
#   silver_tickets            tickety po deduplikacji
#   silver_plans              cennik
#   silver_quarantine         odrzucone wiersze z powodem
#   silver_quarantine_summary podsumowanie kwarantanny (encja, powód, liczba)
from pyspark import pipelines as dp
from pyspark.sql import functions as F

VALID_PLANS = "('P1','P2','P3')"
VALID_INVOICE_STATUS = "('open','paid','failed','uncollectible','void')"
VALID_PRIORITY = "('low','medium','high')"


# ----------------------------------------------------------------------------- pomocnicze
def _flag(df, rules):
    """rules: lista (powód, warunek SQL, który jest PRAWDĄ dla błędnego wiersza). Pierwszy pasujący wygrywa."""
    reason = F.lit(None).cast("string")
    for name, bad in reversed(rules):
        reason = F.when(F.expr(bad), F.lit(name)).otherwise(reason)
    return df.withColumn("_quarantine_reason", reason)


def _known_customers():
    return (
        spark.read.table("bronze_customers")  # noqa: F821
        .where("customer_id IS NOT NULL AND _rescued_data IS NULL")
        .select("customer_id").distinct()
        .withColumn("_known_customer", F.lit(True))
    )


def _flag_unknown_customer(df):
    """Zdarzenie nieznanego klienta trafia do kwarantanny (integralność referencyjna, stream-static join)."""
    joined = df.join(_known_customers(), "customer_id", "left")
    return joined.withColumn(
        "_quarantine_reason",
        F.when(F.col("_quarantine_reason").isNull() & F.col("_known_customer").isNull(),
               F.lit("unknown_customer")).otherwise(F.col("_quarantine_reason")),
    ).drop("_known_customer")


KAFKA_REF = F.concat_ws("/", F.col("kafka_topic"), F.col("kafka_partition").cast("string"),
                        F.col("kafka_offset").cast("string"))


def _quarantine(df, entity, key_expr, payload_expr, ref_expr):
    return df.where("_quarantine_reason IS NOT NULL").select(
        F.lit(entity).alias("entity"),
        F.col("_quarantine_reason").alias("reason"),
        key_expr.cast("string").alias("record_key"),
        payload_expr.alias("raw_payload"),
        F.col("_rescued_data").alias("rescued_data"),
        ref_expr.alias("source_ref"),
        F.col("_ingested_at").alias("bronze_ingested_at"),
        F.current_timestamp().alias("quarantined_at"),
    )


dp.create_streaming_table("silver_quarantine", comment="Odrzucone wiersze ze wszystkich źródeł, z powodem i oryginalną treścią.")


# ----------------------------------------------------------------------------- customers (SCD 1)
def _validated_customers():
    return _flag(spark.readStream.table("bronze_customers"), [  # noqa: F821
        ("rescued_data", "_rescued_data IS NOT NULL"),
        ("null_key", "customer_id IS NULL"),
        ("null_signup_date", "signup_date IS NULL OR updated_at IS NULL"),
    ])


@dp.append_flow(target="silver_quarantine", name="quarantine_customers")
def quarantine_customers():
    payload = F.to_json(F.struct("customer_id", "company_name", "industry", "company_size", "country",
                                 "signup_date", "acquisition_channel", "updated_at", "_rescued_data"))
    return _quarantine(_validated_customers(), "customers", F.col("customer_id"), payload, F.col("_source_file"))


@dp.temporary_view()
def customers_clean():
    return (
        _validated_customers().where("_quarantine_reason IS NULL")
        .select(
            "customer_id",
            F.trim("company_name").alias("company_name"),
            F.expr("coalesce(nullif(trim(industry), ''), 'Unknown')").alias("industry"),
            "company_size",
            F.initcap(F.trim("country")).alias("country"),
            "signup_date",
            "acquisition_channel",
            "updated_at",
        )
    )


dp.create_streaming_table("silver_customers", comment="Klienci, SCD typu 1: zostaje aktualna wersja wiersza.")
dp.create_auto_cdc_flow(
    target="silver_customers",
    source="customers_clean",
    keys=["customer_id"],
    sequence_by="updated_at",
    stored_as_scd_type=1,
)


# ----------------------------------------------------------------------------- subscriptions (SCD 2)
def _validated_subscriptions():
    return _flag(spark.readStream.table("bronze_subscriptions"), [  # noqa: F821
        ("parse_failure", "_json_malformed"),
        ("rescued_data", "_rescued_data IS NOT NULL"),
        ("null_key", "subscription_id IS NULL OR customer_id IS NULL"),
        ("unknown_plan", f"plan_id IS NULL OR plan_id NOT IN {VALID_PLANS}"),
        ("invalid_seats", "seats IS NULL OR seats <= 0"),
        ("invalid_status", "status IS NULL OR lower(trim(status)) NOT IN ('active','churned')"),
        ("null_dates", "start_date IS NULL OR updated_at IS NULL"),
    ])


@dp.append_flow(target="silver_quarantine", name="quarantine_subscriptions")
def quarantine_subscriptions():
    return _quarantine(_validated_subscriptions(), "subscriptions", F.col("kafka_key"), F.col("raw_value"), KAFKA_REF)


@dp.temporary_view()
def subscriptions_clean():
    return (
        _validated_subscriptions().where("_quarantine_reason IS NULL")
        .select("subscription_id", "customer_id", "plan_id", "seats",
                F.lower(F.trim("status")).alias("status"),
                "start_date", "end_date", "churn_reason", "updated_at")
    )


dp.create_streaming_table(
    "silver_subscriptions",
    comment="Subskrypcje, SCD typu 2: nowa wersja przy zmianie planu, liczby miejsc lub statusu.",
)
dp.create_auto_cdc_flow(
    target="silver_subscriptions",
    source="subscriptions_clean",
    keys=["subscription_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
    track_history_column_list=["plan_id", "seats", "status"],
)


# ----------------------------------------------------------------------------- invoices (stan bieżący + spłaszczenie)
def _validated_invoices():
    return _flag(spark.readStream.table("bronze_invoices"), [  # noqa: F821
        ("parse_failure", "_json_malformed"),
        ("rescued_data", "_rescued_data IS NOT NULL"),
        ("null_key", "invoice_id IS NULL OR subscription_id IS NULL OR customer_id IS NULL"),
        ("null_dates", "invoice_date IS NULL OR updated_at IS NULL"),
        ("missing_lines", "lines IS NULL OR size(lines) = 0"),
        ("unknown_status", f"status IS NULL OR status NOT IN {VALID_INVOICE_STATUS}"),
        ("total_mismatch",
         "total IS NULL OR abs(aggregate(lines, 0D, (acc, x) -> acc + x.amount) - coalesce(discount.amount, 0D) - total) > 0.01"),
        ("paid_without_paid_at", "status = 'paid' AND paid_at IS NULL"),
    ])


@dp.append_flow(target="silver_quarantine", name="quarantine_invoices")
def quarantine_invoices():
    return _quarantine(_validated_invoices(), "invoices", F.col("kafka_key"), F.col("raw_value"), KAFKA_REF)


@dp.temporary_view()
def invoices_clean():
    return (
        _validated_invoices().where("_quarantine_reason IS NULL")
        .select("invoice_id", "subscription_id", "customer_id", "invoice_date", "due_date", "lines", "discount",
                "total", "status", "paid_at", "payment_method", "payment_attempts",
                F.size("payment_attempts").alias("attempts_count"), "updated_at")
    )


dp.create_streaming_table("silver_invoices", comment="Faktury: aktualny stan (najnowszy updated_at); historia statusów w Bronze.")
dp.create_auto_cdc_flow(
    target="silver_invoices",
    source="invoices_clean",
    keys=["invoice_id"],
    sequence_by="updated_at",
    stored_as_scd_type=1,
)


@dp.materialized_view(name="silver_invoice_lines", comment="Pozycje faktur (jedna linia = jedna pozycja).")
def silver_invoice_lines():
    return (
        spark.read.table("silver_invoices")  # noqa: F821
        .select("invoice_id", "subscription_id", "customer_id", "invoice_date",
                F.posexplode("lines").alias("pos", "line"))
        .select(
            "invoice_id", "subscription_id", "customer_id", "invoice_date",
            (F.col("pos") + 1).alias("line_no"),
            F.col("line.type").alias("line_type"),
            F.col("line.plan_id").alias("plan_id"),
            F.col("line.quantity").alias("quantity"),
            F.col("line.unit_price").alias("unit_price"),
            F.col("line.amount").alias("amount"),
        )
    )


@dp.materialized_view(name="silver_payment_attempts", comment="Próby płatności faktur (jedna linia = jedna próba).")
def silver_payment_attempts():
    return (
        spark.read.table("silver_invoices")  # noqa: F821
        .select("invoice_id", "subscription_id", "customer_id",
                F.posexplode("payment_attempts").alias("pos", "att"))
        .select(
            "invoice_id", "subscription_id", "customer_id",
            F.col("att.attempt").alias("attempt"),
            F.col("att.at").alias("attempt_at"),
            F.col("att.result").alias("result"),
            F.col("att.reason").alias("decline_reason"),
        )
    )


# ----------------------------------------------------------------------------- usage_events (deduplikacja)
def _validated_usage():
    base = _flag(spark.readStream.table("bronze_usage_events"), [  # noqa: F821
        ("parse_failure", "_json_malformed"),
        ("rescued_data", "_rescued_data IS NOT NULL"),
        ("null_key", "event_id IS NULL OR customer_id IS NULL"),
        ("null_event_ts", "event_ts IS NULL"),
    ])
    return _flag_unknown_customer(base)


@dp.append_flow(target="silver_quarantine", name="quarantine_usage_events")
def quarantine_usage_events():
    return _quarantine(_validated_usage(), "usage_events", F.col("kafka_key"), F.col("raw_value"), KAFKA_REF)


@dp.temporary_view()
def usage_events_clean():
    return (
        _validated_usage().where("_quarantine_reason IS NULL")
        .select("event_id", "customer_id", "event_ts", "event_type",
                F.expr("coalesce(user_id, 'unknown')").alias("user_id"))
    )


dp.create_streaming_table("silver_usage_events", comment="Zdarzenia użycia po deduplikacji (klucz event_id).")
dp.create_auto_cdc_flow(
    target="silver_usage_events",
    source="usage_events_clean",
    keys=["event_id"],
    sequence_by="event_ts",
    stored_as_scd_type=1,
)


# ----------------------------------------------------------------------------- tickets (deduplikacja)
def _validated_tickets():
    base = _flag(spark.readStream.table("bronze_tickets"), [  # noqa: F821
        ("parse_failure", "_json_malformed"),
        ("rescued_data", "_rescued_data IS NOT NULL"),
        ("null_key", "ticket_id IS NULL OR customer_id IS NULL"),
        ("null_created_at", "created_at IS NULL"),
        ("invalid_priority", f"priority IS NULL OR lower(trim(priority)) NOT IN {VALID_PRIORITY}"),
        ("closed_before_created", "closed_at IS NOT NULL AND closed_at < created_at"),
    ])
    return _flag_unknown_customer(base)


@dp.append_flow(target="silver_quarantine", name="quarantine_tickets")
def quarantine_tickets():
    return _quarantine(_validated_tickets(), "tickets", F.col("kafka_key"), F.col("raw_value"), KAFKA_REF)


@dp.temporary_view()
def tickets_clean():
    return (
        _validated_tickets().where("_quarantine_reason IS NULL")
        .select("ticket_id", "customer_id", "created_at", "closed_at",
                F.lower(F.trim("priority")).alias("priority"), "status",
                F.coalesce("closed_at", "created_at").alias("_seq"))
    )


dp.create_streaming_table("silver_tickets", comment="Tickety wsparcia po deduplikacji (klucz ticket_id).")
dp.create_auto_cdc_flow(
    target="silver_tickets",
    source="tickets_clean",
    keys=["ticket_id"],
    sequence_by="_seq",
    except_column_list=["_seq"],
    stored_as_scd_type=1,
)


# ----------------------------------------------------------------------------- plans i podsumowanie jakości
@dp.materialized_view(name="silver_plans", comment="Cennik planów (dane referencyjne).")
def silver_plans():
    return (
        spark.read.table("bronze_plans")  # noqa: F821
        .where("_rescued_data IS NULL AND plan_id IS NOT NULL")
        .dropDuplicates(["plan_id"])
        .select("plan_id", "plan_name", "monthly_price", "seats_included", "extra_seat_price")
    )


@dp.materialized_view(name="silver_quarantine_summary", comment="Ile wierszy odrzucono i dlaczego.")
def silver_quarantine_summary():
    return (
        spark.read.table("silver_quarantine")  # noqa: F821
        .groupBy("entity", "reason")
        .agg(F.count("*").alias("rows"), F.min("bronze_ingested_at").alias("first_seen"))
        .orderBy("entity", "reason")
    )
