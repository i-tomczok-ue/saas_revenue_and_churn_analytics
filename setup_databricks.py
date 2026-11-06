"""
Jednorazowa konfiguracja Databricks z Twojego komputera (przez databricks-sdk):
  1. tworzy schemat `saas` w tym samym katalogu, w którym leży Volume `landing`,
  2. tworzy zakres sekretów `saas-portfolio` i zapisuje w nim dane logowania do Kafki,
  3. wypisuje cztery wartości "Configuration", które wklejasz w ustawieniach pipeline'u.

Uruchomienie (w folderze projektu, przy aktywnym .venv):
    python setup_databricks.py

Wymaga w .env: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_VOLUME_PATH, KAFKA_BOOTSTRAP_SERVERS,
KAFKA_API_KEY, KAFKA_API_SECRET. Jeśli masz osobny klucz Confluent dla Databricks (zalecane), dopisz do .env
DBX_KAFKA_API_KEY i DBX_KAFKA_API_SECRET, a skrypt użyje ich zamiast klucza lokalnego.
Skrypt nigdy nie wypisuje wartości sekretów.
"""
import os
import sys

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

SCOPE = "saas-portfolio"
SCHEMA = "saas"


def main():
    load_dotenv()
    for name in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_VOLUME_PATH", "KAFKA_BOOTSTRAP_SERVERS"):
        if not os.environ.get(name):
            sys.exit(f"Brak {name} w pliku .env")

    parts = os.environ["DATABRICKS_VOLUME_PATH"].strip("/").split("/")
    if len(parts) < 4 or parts[0] != "Volumes":
        sys.exit("DATABRICKS_VOLUME_PATH powinno wyglądać tak: /Volumes/<katalog>/<schemat>/<volume>")
    catalog = parts[1]

    separate = bool(os.environ.get("DBX_KAFKA_API_KEY") and os.environ.get("DBX_KAFKA_API_SECRET"))
    key = os.environ.get("DBX_KAFKA_API_KEY") or os.environ.get("KAFKA_API_KEY")
    secret = os.environ.get("DBX_KAFKA_API_SECRET") or os.environ.get("KAFKA_API_SECRET")
    if not key or not secret:
        sys.exit("Brak KAFKA_API_KEY / KAFKA_API_SECRET w pliku .env")
    if not separate:
        print("Uwaga: używam tego samego klucza Confluent co lokalnie (DBX_KAFKA_API_KEY nie jest ustawiony).")

    w = WorkspaceClient()
    secrets_ok = True

    try:
        w.schemas.create(name=SCHEMA, catalog_name=catalog)
        print(f"Schemat {catalog}.{SCHEMA} utworzony.")
    except Exception as e:  # noqa: BLE001
        if "already exists" in str(e).lower():
            print(f"Schemat {catalog}.{SCHEMA} już istnieje.")
        else:
            print(f"Nie udało się utworzyć schematu (utwórz go ręcznie w Catalog): {e}")

    try:
        w.secrets.create_scope(scope=SCOPE)
        print(f"Zakres sekretów '{SCOPE}' utworzony.")
    except Exception as e:  # noqa: BLE001
        if "already exists" in str(e).lower():
            print(f"Zakres sekretów '{SCOPE}' już istnieje.")
        else:
            secrets_ok = False
            print(f"Nie udało się utworzyć zakresu sekretów: {e}")

    if secrets_ok:
        try:
            w.secrets.put_secret(SCOPE, "kafka-api-key", string_value=key)
            w.secrets.put_secret(SCOPE, "kafka-api-secret", string_value=secret)
            names = sorted(s.key for s in w.secrets.list_secrets(SCOPE))
            print(f"Sekrety zapisane. W zakresie '{SCOPE}' są: {names}")
        except Exception as e:  # noqa: BLE001
            secrets_ok = False
            print(f"Nie udało się zapisać sekretów: {e}")

    key_value = "{{secrets/%s/kafka-api-key}}" % SCOPE if secrets_ok else "<TU_WPISZ_KLUCZ_API>"
    secret_value = "{{secrets/%s/kafka-api-secret}}" % SCOPE if secrets_ok else "<TU_WPISZ_SECRET>"
    print("\nWartości 'Configuration' do ustawień pipeline'u (klucz = wartość):")
    print(f"  landing_path     = {os.environ['DATABRICKS_VOLUME_PATH'].rstrip('/')}")
    print(f"  kafka_bootstrap  = {os.environ['KAFKA_BOOTSTRAP_SERVERS']}")
    print(f"  kafka_api_key    = {key_value}")
    print(f"  kafka_api_secret = {secret_value}")
    print(f"\nDomyślny katalog pipeline'u: {catalog}   domyślny schemat: {SCHEMA}")
    if not secrets_ok:
        print("\nSekrety nie zadziałały: wpisz klucz i secret wprost w Configuration tylko na czas nauki,"
              " a po zakończeniu projektu usuń ten klucz API w Confluent.")


if __name__ == "__main__":
    main()
