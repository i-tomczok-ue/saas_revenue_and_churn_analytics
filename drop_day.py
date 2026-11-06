"""
Wysyła dane wybranego dnia: customers (CSV) i plans do Volume w Databricks oraz
subscriptions, invoices, usage_events i tickets do topików w Confluent Cloud (Kafka).

Przykłady (w folderze projektu, przy aktywnym .venv):
    python drop_day.py --backfill --dry-run     # pokaż, co zostałoby wysłane (nic nie wysyła)
    python drop_day.py --backfill               # historia 2025-01-01 .. 2026-09-30 (wznawia po przerwaniu)
    python drop_day.py --next                   # następny dzień po ostatnio wysłanym (demo dzień po dniu)
    python drop_day.py --next --days 3          # trzy kolejne dni
    python drop_day.py --date 2026-10-05        # konkretny dzień (tylko jeśli jeszcze nie wysłany)
    python drop_day.py --range 2026-10-01 2026-10-07
    python drop_day.py --next --only kafka      # tylko Kafka (albo --only files)

Konfiguracja w pliku .env:
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY, KAFKA_API_SECRET
    DATABRICKS_HOST         np. https://dbc-xxxx.cloud.databricks.com
    DATABRICKS_TOKEN        token osobisty
    DATABRICKS_VOLUME_PATH  np. /Volumes/<katalog>/<schemat>/landing

Układ w Volume:  <VOLUME>/plans/plans.csv  oraz  <VOLUME>/customers/customers_RRRR-MM-DD.csv
Stan wysyłki (ostatni wysłany dzień) jest w output/drop_state.json.
"""
import argparse
import io
import json
import os
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path

OUT = Path("output")
STATE = OUT / "drop_state.json"
BACKFILL_END = date(2026, 9, 30)
TOPICS = {  # topik -> pole użyte jako klucz wiadomości (ten sam klucz = ta sama partycja = zachowana kolejność)
    "subscriptions": "subscription_id",
    "invoices": "invoice_id",
    "usage_events": "customer_id",
    "tickets": "customer_id",
}
DATE_RE = re.compile(r"_(\d{4}-\d{2}-\d{2})\.")


def load_state():
    if STATE.exists():
        return json.loads(STATE.read_text(encoding="utf-8"))
    return {"last_day": None, "plans_sent": False}


def save_state(state):
    STATE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def generated_days():
    days = set()
    for pattern in ("files/customers/*.csv", "kafka/*/*.jsonl"):
        for p in OUT.glob(pattern):
            m = DATE_RE.search(p.name)
            if m:
                days.add(date.fromisoformat(m.group(1)))
    return sorted(days)


def message_key(line, field):
    try:
        v = json.loads(line).get(field)
        return None if v is None else str(v)
    except (ValueError, AttributeError):
        m = re.search(r'"%s"\s*:\s*"([^"]+)"' % field, line)
        return m.group(1) if m else None


def day_range(a, b):
    d = a
    while d <= b:
        yield d
        d += timedelta(days=1)


def main():
    ap = argparse.ArgumentParser(description="Wysyłka danych jednego dnia (pliki do Volume, zdarzenia do Kafki)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--next", action="store_true", help="następny dzień po ostatnio wysłanym")
    g.add_argument("--backfill", action="store_true", help="cała historia do 2026-09-30 (wznawia od ostatniego dnia)")
    g.add_argument("--date", help="jeden dzień, np. 2026-10-05")
    g.add_argument("--range", nargs=2, metavar=("OD", "DO"), help="zakres dni")
    ap.add_argument("--days", type=int, default=1, help="z --next: ile kolejnych dni (domyślnie 1)")
    ap.add_argument("--only", choices=["files", "kafka"], help="wyślij tylko pliki albo tylko Kafkę")
    ap.add_argument("--dry-run", action="store_true", help="nic nie wysyłaj, tylko policz")
    ap.add_argument("--pause", type=float, default=0.0, help="przerwa (s) między dniami")
    ap.add_argument("--force", action="store_true", help="pozwól wysłać dzień ponownie (powstaną duplikaty!)")
    args = ap.parse_args()

    gen = generated_days()
    if not gen:
        sys.exit("Brak danych w output/. Najpierw uruchom: python generate_data.py")
    first_gen, last_gen = gen[0], gen[-1]
    state = load_state()
    last_sent = date.fromisoformat(state["last_day"]) if state["last_day"] else None

    if args.next:
        start = (last_sent + timedelta(days=1)) if last_sent else first_gen
        days = [d for d in day_range(start, start + timedelta(days=args.days - 1)) if d <= last_gen]
        if not days:
            sys.exit(f"Wszystkie wygenerowane dni ({first_gen} .. {last_gen}) są już wysłane.")
    elif args.backfill:
        start = (last_sent + timedelta(days=1)) if last_sent else first_gen
        days = list(day_range(start, BACKFILL_END))
        if not days:
            sys.exit("Backfill jest już wysłany. Dalej użyj: python drop_day.py --next")
    elif args.date:
        days = [date.fromisoformat(args.date)]
    else:
        days = list(day_range(date.fromisoformat(args.range[0]), date.fromisoformat(args.range[1])))

    expected = (last_sent + timedelta(days=1)) if last_sent else first_gen
    if days[0] != expected and not args.force:
        sys.exit(f"Następny dzień do wysłania to {expected}, a wskazano {days[0]}. Pominięcie lub powtórzenie dni "
                 f"zepsuje ciągłość danych (użyj --force, jeśli to celowe; powtórka tworzy duplikaty).")
    if any(d < first_gen or d > last_gen for d in days):
        sys.exit(f"Poza zakresem wygenerowanych danych ({first_gen} .. {last_gen}).")

    do_files = args.only in (None, "files")
    do_kafka = args.only in (None, "kafka")
    producer = client = None
    volume = None

    if not args.dry_run:
        from dotenv import load_dotenv
        load_dotenv()
        if do_kafka:
            from confluent_kafka import Producer
            producer = Producer({
                "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "PLAIN",
                "sasl.username": os.environ["KAFKA_API_KEY"],
                "sasl.password": os.environ["KAFKA_API_SECRET"],
                "enable.idempotence": True,
                "linger.ms": 50,
            })
        if do_files:
            from databricks.sdk import WorkspaceClient
            client = WorkspaceClient()  # korzysta z DATABRICKS_HOST i DATABRICKS_TOKEN z .env
            volume = os.environ["DATABRICKS_VOLUME_PATH"].rstrip("/")

    stats = {"delivered": 0, "errors": 0}

    def on_delivery(err, msg):
        if err:
            stats["errors"] += 1
            if stats["errors"] <= 5:
                print("  błąd dostarczenia:", err)
        else:
            stats["delivered"] += 1

    def upload(local, remote):
        client.files.upload(remote, io.BytesIO(local.read_bytes()), overwrite=True)

    if do_files and not state["plans_sent"] and not args.dry_run:
        upload(OUT / "files" / "plans" / "plans.csv", f"{volume}/plans/plans.csv")
        state["plans_sent"] = True
        save_state(state)
        print("plans.csv wysłany do Volume")

    t0 = time.time()
    total_files = total_msgs = 0
    for d in days:
        ds = d.isoformat()
        n_files = n_msgs = 0
        if do_files:
            f = OUT / "files" / "customers" / f"customers_{ds}.csv"
            if f.exists():
                if not args.dry_run:
                    upload(f, f"{volume}/customers/{f.name}")
                n_files = 1
        if do_kafka:
            for topic, key_field in TOPICS.items():
                f = OUT / "kafka" / topic / f"{topic}_{ds}.jsonl"
                if not f.exists():
                    continue
                for line in f.read_text(encoding="utf-8").split("\n"):
                    line = line.rstrip("\r")
                    if not line.strip():
                        continue
                    n_msgs += 1
                    if args.dry_run:
                        continue
                    key = message_key(line, key_field)
                    while True:
                        try:
                            producer.produce(topic, key=key, value=line.encode("utf-8"), on_delivery=on_delivery)
                            break
                        except BufferError:
                            producer.poll(0.5)
                    producer.poll(0)
        if producer is not None:
            producer.flush(60)
            if stats["errors"]:
                sys.exit(f"Błędy dostarczenia przy dniu {ds}: stan nie został przesunięty, popraw przyczynę i powtórz.")
        total_files += n_files
        total_msgs += n_msgs
        print(f"{ds}: pliki {n_files}, wiadomości {n_msgs}" + ("  (dry-run)" if args.dry_run else ""))
        if not args.dry_run and (not state["last_day"] or d > date.fromisoformat(state["last_day"])):
            state["last_day"] = ds
            save_state(state)
        if args.pause and not args.dry_run:
            time.sleep(args.pause)

    print(f"\nGotowe: {len(days)} dni, {total_files} plików, {total_msgs} wiadomości, "
          f"{time.time() - t0:.0f} s" + ("  (dry-run, nic nie wysłano)" if args.dry_run else ""))


if __name__ == "__main__":
    main()
