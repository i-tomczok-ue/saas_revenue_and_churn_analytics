# Databricks: plik źródłowy pipeline'u (Lakeflow Spark Declarative Pipelines), warstwa BRONZE.
#
# Bronze przechowuje wszystko, co przyszło (także błędne i zduplikowane wiersze), z metadanymi dostawy:
#   * customers, plans  <- pliki CSV w Volume, wczytywane Auto Loaderem (kolumna _rescued_data)
#   * subscriptions, invoices, usage_events, tickets  <- Kafka (Confluent Cloud), wiadomości JSON
#       raw_value        oryginalny tekst wiadomości
#       pola z JSON-a    sparsowane schematem; wartości o złym typie trafiają do _rescued_data
#       _json_malformed  true, gdy wiadomość w ogóle nie jest poprawnym JSON-em
#
# Konfiguracja pipeline'u (Settings -> Configuration), wypisuje ją setup_databricks.py:
#   landing_path, kafka_bootstrap, kafka_api_key, kafka_api_secret
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DateType, DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

SECRET_SCOPE = "saas-portfolio"
LANDING = spark.conf.get("landing_path").rstrip("/")  # noqa: F821
KAFKA_SERVERS = spark.conf.get("kafka_bootstrap")  # noqa: F821


def _credential(conf_key, secret_key):
    """Najpierw wartość z Configuration (może być odwołaniem {{secrets/...}}), w razie potrzeby dbutils.secrets."""
    value = spark.conf.get(conf_key, None)  # noqa: F821
    if value and not value.startswith("{{secrets/"):
        return value
    return dbutils.secrets.get(scope=SECRET_SCOPE, key=secret_key)  # noqa: F821


_JAAS = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{_credential("kafka_api_key", "kafka-api-key")}" '
    f'password="{_credential("kafka_api_secret", "kafka-api-secret")}";'
)


def S(name, dtype=StringType()):
    return StructField(name, dtype, True)


RESCUED = S("_rescued_data")

CUSTOMERS_SCHEMA = StructType([
    S("customer_id"), S("company_name"), S("industry"), S("company_size"), S("country"),
    S("signup_date", DateType()), S("acquisition_channel"), S("updated_at", TimestampType()),
])
PLANS_SCHEMA = StructType([
    S("plan_id"), S("plan_name"), S("monthly_price", DoubleType()), S("seats_included", IntegerType()),
    S("extra_seat_price", DoubleType()),
])
SUBSCRIPTIONS_SCHEMA = StructType([
    S("subscription_id"), S("customer_id"), S("plan_id"), S("seats", IntegerType()), S("status"),
    S("start_date", DateType()), S("end_date", DateType()), S("churn_reason"),
    S("updated_at", TimestampType()), RESCUED,
])
INVOICES_SCHEMA = StructType([
    S("invoice_id"), S("subscription_id"), S("customer_id"),
    S("invoice_date", DateType()), S("due_date", DateType()),
    S("lines", ArrayType(StructType([
        S("type"), S("plan_id"), S("quantity", IntegerType()), S("unit_price", DoubleType()),
        S("amount", DoubleType()),
    ]))),
    S("discount", StructType([S("code"), S("amount", DoubleType())])),
    S("total", DoubleType()), S("status"), S("paid_at", TimestampType()), S("payment_method"),
    S("payment_attempts", ArrayType(StructType([
        S("attempt", IntegerType()), S("at", TimestampType()), S("result"), S("reason"),
    ]))),
    S("updated_at", TimestampType()), RESCUED,
])
USAGE_SCHEMA = StructType([
    S("event_id"), S("customer_id"), S("event_ts", TimestampType()), S("event_type"), S("user_id"), RESCUED,
])
TICKETS_SCHEMA = StructType([
    S("ticket_id"), S("customer_id"), S("created_at", TimestampType()), S("closed_at", TimestampType()),
    S("priority"), S("status"), RESCUED,
])

JSON_OPTIONS = {
    "rescuedDataColumn": "_rescued_data",
    "mode": "PERMISSIVE",
    "dateFormat": "yyyy-MM-dd",
    "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss",
}


# ----------------------------------------------------------------------------- pliki (Auto Loader)
def _csv_stream(folder, schema):
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .option("dateFormat", "yyyy-MM-dd")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
        .schema(schema)
        .load(f"{LANDING}/{folder}/")
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(name="bronze_customers", comment="Klienci z dziennych plików CSV (Auto Loader), bez czyszczenia.")
@dp.expect("brak_rescued_data", "_rescued_data IS NULL")
def bronze_customers():
    return _csv_stream("customers", CUSTOMERS_SCHEMA).withColumn(
        "_delivery_date",
        F.to_date(F.regexp_extract("_source_file", r"customers_(\d{4}-\d{2}-\d{2})\.csv", 1)),
    )


@dp.table(name="bronze_plans", comment="Cennik planów z pliku CSV (dane referencyjne).")
@dp.expect("brak_rescued_data", "_rescued_data IS NULL")
def bronze_plans():
    return _csv_stream("plans", PLANS_SCHEMA)


# ----------------------------------------------------------------------------- Kafka
def _kafka_stream(topic, schema):
    raw = (
        spark.readStream.format("kafka")  # noqa: F821
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", _JAAS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_value"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
    )
    return (
        raw.withColumn("data", F.from_json("raw_value", schema, JSON_OPTIONS))
        .select("kafka_key", "raw_value", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
                "data.*")
        .withColumn("_json_malformed", F.get_json_object("raw_value", "$").isNull())
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(name="bronze_subscriptions", comment="Wersje subskrypcji z Kafki (źródło SCD 2).")
@dp.expect("json_poprawny", "NOT _json_malformed")
@dp.expect("brak_rescued_data", "_rescued_data IS NULL")
def bronze_subscriptions():
    return _kafka_stream("subscriptions", SUBSCRIPTIONS_SCHEMA)


@dp.table(name="bronze_invoices", comment="Wersje faktur (zagnieżdżony JSON) z Kafki; każda zmiana statusu to nowy dokument.")
@dp.expect("json_poprawny", "NOT _json_malformed")
@dp.expect("brak_rescued_data", "_rescued_data IS NULL")
def bronze_invoices():
    return _kafka_stream("invoices", INVOICES_SCHEMA)


@dp.table(name="bronze_usage_events", comment="Zdarzenia użycia produktu z Kafki.")
@dp.expect("json_poprawny", "NOT _json_malformed")
@dp.expect("brak_rescued_data", "_rescued_data IS NULL")
def bronze_usage_events():
    return _kafka_stream("usage_events", USAGE_SCHEMA)


@dp.table(name="bronze_tickets", comment="Tickety wsparcia z Kafki (wiersz w dniu zamknięcia).")
@dp.expect("json_poprawny", "NOT _json_malformed")
@dp.expect("brak_rescued_data", "_rescued_data IS NULL")
def bronze_tickets():
    return _kafka_stream("tickets", TICKETS_SCHEMA)
