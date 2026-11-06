"""
Generator danych SaaS v2 (Python -> Kafka / pliki -> Databricks -> Power BI).

Uruchomienie (w folderze projektu, przy aktywnym .venv):
    python generate_data.py                    # 500 klientów, dane do 2026-12-31
    python generate_data.py --customers 100    # mniejszy zestaw do testów
    python generate_data.py --clean            # bez celowo brudnych danych
    python generate_data.py --end 2026-11-30   # krótszy okres

Wynik w folderze output/:
    files/plans/plans.csv                          -> Volume (jednorazowo)
    files/customers/customers_RRRR-MM-DD.csv       -> Volume, Auto Loader (CSV, SCD 1)
    kafka/subscriptions/subscriptions_<dzień>.jsonl   -> topik subscriptions (SCD 2)
    kafka/invoices/invoices_<dzień>.jsonl             -> topik invoices (zagnieżdżony JSON, cykl życia)
    kafka/usage_events/usage_events_<dzień>.jsonl     -> topik usage_events
    kafka/tickets/tickets_<dzień>.jsonl               -> topik tickets
    dirty_manifest.csv       lista każdej celowo podłożonej usterki (do weryfikacji pipeline'u)
    ground_truth_mrr.csv     oczekiwane miesięczne MRR (kontraktowe), do porównania z warstwą Gold

Historia 2025-01-01 .. 2026-09-30 to backfill, od 2026-10-01 dane wpadają dzień po dniu (drop_day.py).

Zasady symulacji:
  * subscription_id jest stały (jeden klient = jedna subskrypcja), a każda zmiana planu, liczby miejsc
    lub statusu to nowa wersja z nowszym updated_at (z tego Silver buduje SCD 2).
  * customers: literówka w nazwie firmy i późniejsza korekta (z tego Silver robi SCD 1).
  * faktura trafia do strumienia przy wystawieniu i ponownie przy każdej zmianie statusu.
    Dozwolone ścieżki (tylko do przodu):
        paid | open -> paid | open -> failed -> paid | open -> failed -> uncollectible | open -> void
  * faktura 'uncollectible' kończy subskrypcję (churn_reason = payment_failure).
  * MRR jest kontraktowy (cennik + dopłaty za miejsca), niezależny od płatności; rabat jest osobną miarą.
  * w 3 miesiącach przed rezygnacją spada aktywność użytkowników i rośnie liczba ticketów.
"""
import argparse
import calendar
import copy
import csv
import json
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
from faker import Faker

SEED = 42
SIM_START = date(2025, 1, 1)
BACKFILL_END = date(2026, 9, 30)
DEFAULT_END = date(2026, 12, 31)
OUT = Path("output")

# plan_id, plan_name, monthly_price, seats_included, extra_seat_price
PLANS = [
    ("P1", "Starter", 49.00, 3, 10.00),
    ("P2", "Growth", 199.00, 15, 8.00),
    ("P3", "Enterprise", 799.00, 100, 6.00),
]
PRICE = {p[0]: p[2] for p in PLANS}
INCLUDED = {p[0]: p[3] for p in PLANS}
EXTRA = {p[0]: p[4] for p in PLANS}
NEXT_PLAN = {"P1": "P2", "P2": "P3"}
PREV_PLAN = {"P2": "P1", "P3": "P2"}
BASE_HAZARD = {"P1": 0.035, "P2": 0.018, "P3": 0.008}  # miesięczne prawdopodobieństwo rezygnacji dobrowolnej

CHANNELS = ["Organic Search", "Paid Ads", "Referral", "Outbound Sales", "Partner"]
CHANNEL_W = [0.30, 0.25, 0.20, 0.15, 0.10]
CHANNEL_CHURN = {"Organic Search": 1.0, "Paid Ads": 1.5, "Referral": 0.7, "Outbound Sales": 0.8, "Partner": 0.9}
INDUSTRIES = ["Software", "Marketing", "Logistics", "Legal", "Finance", "Healthcare", "Retail", "Education",
              "Manufacturing"]
SIZES = ["1-10", "11-50", "51-200", "201-1000"]
SIZE_W = [0.40, 0.35, 0.20, 0.05]
PLAN_BY_SIZE = {"1-10": [0.85, 0.15, 0.0], "11-50": [0.35, 0.60, 0.05], "51-200": [0.10, 0.65, 0.25],
                "201-1000": [0.0, 0.30, 0.70]}
COUNTRIES = ["Poland", "Germany", "United Kingdom", "France", "Netherlands", "Czechia", "Spain", "Sweden", "Italy"]
COUNTRY_W = [0.35, 0.20, 0.10, 0.08, 0.07, 0.07, 0.05, 0.04, 0.04]
EVENT_TYPES = ["login", "report_created", "dashboard_viewed", "export_data", "api_call"]
EVENT_W = [0.45, 0.20, 0.20, 0.10, 0.05]
PRIORITIES = ["low", "medium", "high"]
PRIORITY_W = [0.50, 0.35, 0.15]
RESOLUTION_HOURS = {"low": 24.0, "medium": 12.0, "high": 5.0}
DECLINE_REASONS = ["insufficient_funds", "card_expired", "do_not_honor"]
CUSTOMER_FIELDS = ["customer_id", "company_name", "industry", "company_size", "country", "signup_date",
                   "acquisition_channel", "updated_at"]


# ----------------------------------------------------------------------------- pomocnicze
class Rec:
    """Jeden rekord do dostarczenia: dzień dostawy, czas (do sortowania), klucz, dokument lub surowy tekst."""
    __slots__ = ("day", "ts", "key", "doc", "text")

    def __init__(self, day, ts, key, doc=None, text=None):
        self.day, self.ts, self.key, self.doc, self.text = day, ts, key, doc, text

    def line(self):
        if self.text is not None:
            return self.text
        return json.dumps(self.doc, ensure_ascii=False, separators=(",", ":"))


def am_of(d):
    return d.year * 12 + d.month - 1


def am_start(am):
    return date(am // 12, am % 12 + 1, 1)


def am_end(am):
    y, m = am // 12, am % 12 + 1
    return date(y, m, calendar.monthrange(y, m)[1])


def at(d, h=0, mi=0, s=0):
    return datetime(d.year, d.month, d.day, h, mi, s)


def iso(dt):
    return dt.isoformat(timespec="seconds")


def bump(ts):
    return iso(datetime.fromisoformat(ts) + timedelta(seconds=1))


def typo(name, rng):
    idx = [k for k, ch in enumerate(name) if ch.isalpha()]
    k = int(rng.choice(idx))
    mode = int(rng.integers(0, 3))
    if mode == 1 and k + 1 < len(name):
        out = name[:k] + name[k + 1] + name[k] + name[k + 2:]
    elif mode == 2:
        out = name[:k + 1] + name[k] + name[k + 1:]
    else:
        out = name[:k] + name[k + 1:]
    return out if out != name else name[:k] + name[k + 1:]


def invoice_lines(plan, seats):
    lines = [{"type": "plan", "plan_id": plan, "quantity": 1, "unit_price": PRICE[plan], "amount": PRICE[plan]}]
    extra = max(0, seats - INCLUDED[plan])
    if extra:
        lines.append({"type": "extra_seats", "plan_id": plan, "quantity": extra, "unit_price": EXTRA[plan],
                      "amount": round(extra * EXTRA[plan], 2)})
    return lines


def attempt(n, dt, result, reason):
    return {"attempt": n, "at": iso(dt), "result": result, "reason": reason}


# ----------------------------------------------------------------------------- symulacja
def simulate(n_customers, end, rng, fake):
    first_sim_am, end_am = am_of(SIM_START), am_of(end)
    n_months = end_am - first_sim_am + 1
    END_DT = datetime(end.year, end.month, end.day, 23, 59, 59)

    w = np.linspace(1.0, 2.0, n_months)
    w /= w.sum()
    signup_am = rng.choice(n_months, size=n_customers, p=w) + first_sim_am
    signup_day = rng.integers(1, 29, size=n_customers)
    order = np.lexsort((signup_day, signup_am))
    signup_am, signup_day = signup_am[order], signup_day[order]

    customers, subs, inv_specs, ticket_raw = [], [], [], []
    u_cid, u_ts, u_type, u_user = [], [], [], []
    gt = {k: np.zeros(n_months) for k in ("mrr", "new", "exp", "con", "churn", "churn_pf", "active")}
    stats = {"churn_voluntary": 0, "churn_payment_failure": 0}

    for i in range(n_customers):
        cid, sid = f"C{i + 1:04d}", f"S{i + 1:04d}"
        f_am, bday = int(signup_am[i]), int(signup_day[i])
        signup = am_start(f_am).replace(day=bday)
        if signup > end:
            continue

        size = str(rng.choice(SIZES, p=SIZE_W))
        channel = str(rng.choice(CHANNELS, p=CHANNEL_W))
        plan = str(rng.choice(["P1", "P2", "P3"], p=PLAN_BY_SIZE[size]))
        seats = max(1, int(round(INCLUDED[plan] * rng.uniform(0.4, 1.1))))
        country = str(rng.choice(COUNTRIES, p=COUNTRY_W))
        name = fake.company()
        has_typo = rng.random() < 0.04
        method = str(rng.choice(["card", "bank_transfer", "paypal"], p=[0.70, 0.25, 0.05]))
        has_discount = rng.random() < 0.15
        base_util = float(rng.beta(5, 3))

        # --- klient (SCD 1): wiersz przy rejestracji, ewentualnie korekta literówki
        base = dict(customer_id=cid, industry=str(rng.choice(INDUSTRIES)), company_size=size, country=country,
                    signup_date=signup.isoformat(), acquisition_channel=channel)
        t_reg = at(signup, 4, int(rng.integers(0, 60)), 0)
        shown = typo(name, rng) if has_typo else name
        customers.append(Rec(signup, iso(t_reg), cid, dict(base, company_name=shown, updated_at=iso(t_reg))))
        if has_typo:
            fix_day = signup + timedelta(days=int(rng.integers(2, 21)))
            if fix_day <= end:
                t_fix = at(fix_day, 10, 0, 0)
                customers.append(Rec(fix_day, iso(t_fix), cid, dict(base, company_name=name, updated_at=iso(t_fix))))

        # --- życie klienta: rezygnacja dobrowolna (rozkład geometryczny po miesiącach)
        life = int(rng.geometric(BASE_HAZARD[plan] * CHANNEL_CHURN[channel]))
        last_vol_am = f_am + life - 1
        if last_vol_am < end_am:
            d_vol = am_start(last_vol_am + 1)
        else:
            d_vol, last_vol_am = None, end_am

        # --- wersje subskrypcji (zmiany wchodzą w życie w dniu rozliczenia)
        state = {f_am: (plan, seats)}
        versions = [(at(signup, 5, 0, 0), plan, seats)]
        for am in range(f_am + 1, last_vol_am + 1):
            to_end = last_vol_am - am if d_vol else 99
            prev = (plan, seats)
            r = rng.random()
            if r < 0.05 and to_end > 1:
                seats += int(rng.integers(1, 6))
            elif r < 0.07 and to_end > 1 and plan in NEXT_PLAN:
                plan = NEXT_PLAN[plan]
                seats = max(seats, INCLUDED[plan])
            elif r < 0.09 and seats > 1:
                seats = max(1, seats - int(rng.integers(1, 4)))
            elif r < 0.10 and plan in PREV_PLAN:
                plan = PREV_PLAN[plan]
                seats = min(seats, INCLUDED[plan] * 2)
            if (plan, seats) != prev:
                versions.append((at(am_start(am).replace(day=bday), 5, 0, 0), plan, seats))
            state[am] = (plan, seats)

        # --- faktury i ich cykl życia
        d_pf = None
        for am in range(f_am, last_vol_am + 1):
            issue = am_start(am).replace(day=bday)
            if issue > end or (d_pf is not None and issue >= d_pf):
                break
            p, s = state[am]
            lines = invoice_lines(p, s)
            list_total = round(sum(x["amount"] for x in lines), 2)
            disc = round(list_total * 0.10, 2) if has_discount else 0.0
            discount = {"code": "PROMO10" if has_discount else None, "amount": disc}
            total = round(list_total - disc, 2)
            t0 = at(issue, 6, 0, 0)
            reason = str(rng.choice(DECLINE_REASONS))
            if d_pf is not None:  # poprzednia faktura została odpisana, ta zostanie anulowana
                events = [(t0, "open", [], None), (at(d_pf, 6, 0, 0), "void", [], None)]
            else:
                r = rng.random()
                if r < 0.85:  # zapłacona w dniu wystawienia
                    tp = t0 + timedelta(minutes=int(rng.integers(1, 240)))
                    events = [(tp, "paid", [attempt(1, tp, "succeeded", None)], tp)]
                elif r < 0.955:  # zapłacona po 1-10 dniach
                    tp = at(issue + timedelta(days=int(rng.integers(1, 11))), int(rng.integers(9, 18)),
                            int(rng.integers(0, 60)), 0)
                    events = [(t0, "open", [], None), (tp, "paid", [attempt(1, tp, "succeeded", None)], tp)]
                elif r < 0.995:  # pierwsza płatność odrzucona
                    t1 = at(issue + timedelta(days=int(rng.integers(1, 4))), int(rng.integers(9, 18)),
                            int(rng.integers(0, 60)), 0)
                    a1 = attempt(1, t1, "declined", reason)
                    events = [(t0, "open", [], None), (t1, "failed", [a1], None)]
                    t2 = t1 + timedelta(days=int(rng.integers(3, 11)))
                    if rng.random() < 0.60:  # odzyskana przy ponownej próbie
                        events.append((t2, "paid", [a1, attempt(2, t2, "succeeded", None)], t2))
                    else:  # druga próba też odrzucona, po 30 dniach odpis
                        a2 = attempt(2, t2, "declined", str(rng.choice(DECLINE_REASONS)))
                        events.append((t2, "failed", [a1, a2], None))
                        d_pf = issue + timedelta(days=30)
                        events.append((at(d_pf, 6, 0, 0), "uncollectible", [a1, a2], None))
                else:  # anulowana
                    tv = at(issue + timedelta(days=int(rng.integers(1, 6))), 10, 0, 0)
                    events = [(t0, "open", [], None), (tv, "void", [], None)]
            inv_specs.append(dict(cid=cid, sid=sid, issue=issue, due=issue + timedelta(days=14), lines=lines,
                                  discount=discount, total=total, method=method, events=events))

        # --- ostateczna data rezygnacji: wcześniejsza z dwóch ścieżek
        if d_pf is not None and (d_vol is None or d_pf < d_vol):
            d_churn, reason_c = d_pf, "payment_failure"
        elif d_vol is not None:
            d_churn, reason_c = d_vol, "voluntary"
        else:
            d_churn, reason_c = None, None
        if d_churn is not None and d_churn <= end:
            stats["churn_voluntary" if reason_c == "voluntary" else "churn_payment_failure"] += 1

        # --- wiersze subskrypcji (SCD 2)
        def sub_doc(p, s, status, end_date, churn_reason, dt):
            return {"subscription_id": sid, "customer_id": cid, "plan_id": p, "seats": s, "status": status,
                    "start_date": signup.isoformat(), "end_date": end_date, "churn_reason": churn_reason,
                    "updated_at": iso(dt)}

        kept = [v for v in versions if d_churn is None or v[0].date() < d_churn]
        for dt, p, s in kept:
            if dt <= END_DT:
                subs.append(Rec(dt.date(), iso(dt), sid, sub_doc(p, s, "active", None, None, dt)))
        if d_churn is not None:
            dt = at(d_churn, 6, 1, 0) if reason_c == "payment_failure" else at(d_churn, 0, 5, 0)
            if dt <= END_DT:
                p, s = kept[-1][1], kept[-1][2]
                subs.append(Rec(dt.date(), iso(dt), sid,
                                sub_doc(p, s, "churned", (d_churn - timedelta(days=1)).isoformat(), reason_c, dt)))

        # --- MRR kontraktowy (do pliku ground truth)
        prev_mrr = 0.0
        for am in range(f_am, end_am + 1):
            ref_end = min(am_end(am), end)  # przy --end w środku miesiąca ostatni miesiąc liczymy na dzień końca
            active = d_churn is None or d_churn > ref_end
            cur = 0.0
            if active:
                sm = am
                if am > f_am and am_end(am) > end and am_start(am).replace(day=bday) > end:
                    sm = am - 1  # zmiana planu z dnia rozliczenia jeszcze się nie wydarzyła
                p, s = state[sm]
                cur = PRICE[p] + max(0, s - INCLUDED[p]) * EXTRA[p]
            k = am - first_sim_am
            if prev_mrr == 0 and cur > 0:
                gt["new"][k] += cur
            elif prev_mrr > 0 and cur == 0:
                gt["churn"][k] += prev_mrr
                if reason_c == "payment_failure":
                    gt["churn_pf"][k] += prev_mrr
            elif cur > prev_mrr:
                gt["exp"][k] += cur - prev_mrr
            elif cur < prev_mrr:
                gt["con"][k] += prev_mrr - cur
            gt["mrr"][k] += cur
            gt["active"][k] += 1 if cur > 0 else 0
            prev_mrr = cur
            if not active:
                break

        # --- użycie produktu i tickety w miesiącach świadczenia usługi
        last_service = min((d_churn - timedelta(days=1)) if d_churn else end, end)
        last_svc_am = am_of(last_service)
        churn_visible = d_churn is not None and d_churn <= end
        for am in range(f_am, last_svc_am + 1):
            p, s = state[am]
            to_end = (last_svc_am - am) if churn_visible else 99
            factor = (0.3, 0.5, 0.75)[to_end] if to_end <= 2 else 1.0
            lo, hi = max(am_start(am), signup), min(am_end(am), last_service)
            span = (hi - lo).days + 1
            if span <= 0:
                continue
            active_n = int(np.clip(round(s * base_util * factor), 1, s))
            counts = 1 + rng.poisson(2.0 * factor, size=active_n)
            total_ev = int(counts.sum())
            day_off = rng.integers(0, span, size=total_ev)
            secs = rng.integers(7 * 3600, 19 * 3600, size=total_ev)
            lo64 = np.datetime64(lo.isoformat(), "s")
            u_ts.append(lo64 + (day_off * 86400 + secs).astype("timedelta64[s]"))
            u_cid.append(np.full(total_ev, cid))
            u_type.append(rng.choice(EVENT_TYPES, p=EVENT_W, size=total_ev))
            u_user.extend(f"U{i + 1:04d}-{u:03d}" for u in np.repeat(np.arange(1, active_n + 1), counts))

            boost = (4.0, 3.0, 2.0)[to_end] if to_end <= 2 else 1.0
            lam = (0.10 + 0.012 * np.sqrt(s)) * boost
            for _ in range(int(rng.poisson(lam))):
                created = datetime(lo.year, lo.month, lo.day) + timedelta(
                    days=int(rng.integers(0, span)), seconds=int(rng.integers(8 * 3600, 18 * 3600)))
                prio = str(rng.choice(PRIORITIES, p=PRIORITY_W))
                closed = created + timedelta(hours=1 + float(rng.exponential(RESOLUTION_HOURS[prio])))
                ticket_raw.append((created, closed if closed <= END_DT else None, cid, prio))

    # --- faktury: numeracja po dacie wystawienia, potem dokumenty dla każdej wersji
    invoices = []
    inv_specs.sort(key=lambda sp: (sp["issue"], sp["sid"]))
    for n, sp in enumerate(inv_specs, start=1):
        iid = f"INV-{n:06d}"
        for dt, status, attempts, paid_at in sp["events"]:
            if dt > END_DT:
                continue
            doc = {"invoice_id": iid, "subscription_id": sp["sid"], "customer_id": sp["cid"],
                   "invoice_date": sp["issue"].isoformat(), "due_date": sp["due"].isoformat(),
                   "lines": sp["lines"], "discount": sp["discount"], "total": sp["total"], "status": status,
                   "paid_at": iso(paid_at) if paid_at else None, "payment_method": sp["method"],
                   "payment_attempts": attempts, "updated_at": iso(dt)}
            invoices.append(Rec(dt.date(), iso(dt), iid, doc))

    # --- usage_events
    usage = []
    if u_ts:
        ts_all, cid_all = np.concatenate(u_ts), np.concatenate(u_cid)
        type_all, user_all = np.concatenate(u_type), np.array(u_user)
        o = np.argsort(ts_all, kind="stable")
        ts_s = np.datetime_as_string(ts_all[o], unit="s")
        for n, j in enumerate(o):
            t = str(ts_s[n])
            doc = {"event_id": f"E{n + 1:07d}", "customer_id": str(cid_all[j]), "event_ts": t,
                   "event_type": str(type_all[j]), "user_id": str(user_all[j])}
            usage.append(Rec(date.fromisoformat(t[:10]), t, doc["customer_id"], doc))

    # --- tickets: wiersz trafia do strumienia w dniu zamknięcia (lub utworzenia, jeśli nadal otwarty)
    tickets = []
    ticket_raw.sort(key=lambda x: x[0])
    for n, (created, closed, cid, prio) in enumerate(ticket_raw, start=1):
        emit = closed or created
        doc = {"ticket_id": f"T{n:05d}", "customer_id": cid, "created_at": iso(created),
               "closed_at": iso(closed) if closed else None, "priority": prio,
               "status": "closed" if closed else "open"}
        tickets.append(Rec(emit.date(), iso(emit), cid, doc))

    gt_rows = []
    for k in range(n_months):
        m = am_start(first_sim_am + k)
        gt_rows.append([m.strftime("%Y-%m"), round(gt["mrr"][k], 2), round(gt["new"][k], 2), round(gt["exp"][k], 2),
                        round(gt["con"][k], 2), round(gt["churn"][k], 2), round(gt["churn_pf"][k], 2),
                        int(gt["active"][k])])
    return customers, subs, invoices, usage, tickets, gt_rows, stats


# ----------------------------------------------------------------------------- brudne dane
def sample_idx(n, frac, rng):
    if n == 0:
        return []
    k = max(1, int(round(n * frac)))
    return rng.choice(n, size=min(k, n), replace=False).tolist()


def inject_dirty(customers, subs, invoices, usage, tickets, rng, end):
    manifest = []

    def mf(day, entity, channel, key, kind, column, bad, handling):
        manifest.append([day.isoformat(), entity, channel, key, kind, column, str(bad), handling])

    def dups(recs, entity, channel, frac_same, frac_next):
        extra = []
        for i in sample_idx(len(recs), frac_same, rng):
            r = recs[i]
            extra.append(Rec(r.day, r.ts, r.key, copy.deepcopy(r.doc), r.text))
            mf(r.day, entity, channel, r.key, "duplicate_same_file", "*", "exact copy", "dedup")
        for i in sample_idx(len(recs), frac_next, rng):
            r = recs[i]
            if r.day + timedelta(days=1) <= end:
                extra.append(Rec(r.day + timedelta(days=1), r.ts, r.key, copy.deepcopy(r.doc), r.text))
                mf(r.day + timedelta(days=1), entity, channel, r.key, "duplicate_next_day", "*",
                   "exact copy redelivered", "dedup")
        return extra

    def malformed(recs, entity, channel, frac):
        extra = []
        for i in sample_idx(len(recs), frac, rng):
            r = recs[i]
            full = r.line()
            extra.append(Rec(r.day, r.ts, r.key, None, full[: int(len(full) * 0.6)]))
            mf(r.day, entity, channel, r.key, "malformed_json", "*", "truncated message", "parse_failure")
        return extra

    # ---- customers (CSV)
    base, extra = list(customers), []
    for i in sample_idx(len(base), 0.012, rng):
        r = base[i]
        d = dict(r.doc)
        true = date.fromisoformat(d["signup_date"])
        bad = [true.strftime("%d/%m/%Y"), "n/a", "2025-13-45"][int(rng.integers(0, 3))]
        d["signup_date"], d["updated_at"] = bad, bump(d["updated_at"])
        extra.append(Rec(r.day, d["updated_at"], r.key, d))
        mf(r.day, "customers", "file", r.key, "wrong_type", "signup_date", bad, "rescued_data")
    for i in sample_idx(len(base), 0.02, rng):
        r = base[i]
        r.doc["industry"] = ""
        mf(r.day, "customers", "file", r.key, "missing_value", "industry", "(empty)", "null_handling")
    for i in sample_idx(len(base), 0.02, rng):
        r = base[i]
        if rng.random() < 0.5:
            r.doc["company_name"] = "  " + r.doc["company_name"] + "  "
            col, bad = "company_name", "padded with spaces"
        else:
            r.doc["country"] = {"Poland": " poland"}.get(r.doc["country"], r.doc["country"].upper() + " ")
            col, bad = "country", r.doc["country"]
        mf(r.day, "customers", "file", r.key, "whitespace_or_case", col, bad, "trim_normalize")
    extra += dups(base, "customers", "file", 0.008, 0.008)
    customers += extra

    # ---- subscriptions (JSON, Kafka)
    base, extra = list(subs), []
    for i in sample_idx(len(base), 0.02, rng):
        r = base[i]
        d = copy.deepcopy(r.doc)
        v = int(rng.integers(0, 3))
        if v == 0:
            d["seats"], col, bad = "ten", "seats", "ten"
        elif v == 1:
            d["seats"], col, bad = 3.5, "seats", 3.5
        else:
            d["start_date"], col, bad = "2025-13-45", "start_date", "2025-13-45"
        d["updated_at"] = bump(d["updated_at"])
        extra.append(Rec(r.day, d["updated_at"], r.key, d))
        mf(r.day, "subscriptions", "kafka", r.key, "wrong_type", col, bad, "rescued_data")
    for kind, frac, col, val, handling in (("negative_seats", 0.01, "seats", -2, "quarantine"),
                                           ("unknown_plan", 0.01, "plan_id", "P9", "quarantine"),
                                           ("status_case", 0.01, "status", "<upper+space>", "trim_normalize"),
                                           ("null_key", 0.006, "customer_id", None, "quarantine")):
        for i in sample_idx(len(base), frac, rng):
            r = base[i]
            d = copy.deepcopy(r.doc)
            if kind == "status_case":
                val = r.doc["status"].upper() + " "
            d[col] = val
            d["updated_at"] = bump(d["updated_at"])
            extra.append(Rec(r.day, d["updated_at"], r.key, d))
            mf(r.day, "subscriptions", "kafka", r.key, kind, col, val, handling)
    extra += malformed(base, "subscriptions", "kafka", 0.006)
    extra += dups(base, "subscriptions", "kafka", 0.008, 0.008)
    subs += extra

    # ---- invoices (zagnieżdżony JSON, Kafka)
    base, extra = list(invoices), []
    for i in sample_idx(len(base), 0.012, rng):
        r = base[i]
        d = copy.deepcopy(r.doc)
        v = int(rng.integers(0, 3))
        if v == 0:
            d["total"], col, bad = "N/A", "total", "N/A"
        elif v == 1:
            bad = str(d["total"]).replace(".", ",") + " zł"
            d["total"], col = bad, "total"
        elif d.get("lines"):
            d["lines"][0]["quantity"], col, bad = "five", "lines[0].quantity", "five"
        else:
            d["invoice_date"], col, bad = "2025-13-45", "invoice_date", "2025-13-45"
        d["updated_at"] = bump(d["updated_at"])
        extra.append(Rec(r.day, d["updated_at"], r.key, d))
        mf(r.day, "invoices", "kafka", r.key, "wrong_type", col, bad, "rescued_data")
    for i in sample_idx(len(base), 0.006, rng):
        r = base[i]
        d = copy.deepcopy(r.doc)
        if rng.random() < 0.5:
            d.pop("lines")
            bad = "(missing)"
        else:
            d["lines"], bad = [], "[]"
        d["updated_at"] = bump(d["updated_at"])
        extra.append(Rec(r.day, d["updated_at"], r.key, d))
        mf(r.day, "invoices", "kafka", r.key, "missing_or_empty_lines", "lines", bad, "quarantine")
    for i in sample_idx(len(base), 0.008, rng):
        r = base[i]
        d = copy.deepcopy(r.doc)
        d["total"] = round(d["total"] + 10, 2)
        d["updated_at"] = bump(d["updated_at"])
        extra.append(Rec(r.day, d["updated_at"], r.key, d))
        mf(r.day, "invoices", "kafka", r.key, "total_mismatch", "total", d["total"], "quarantine")
    paid_idx = [i for i, r in enumerate(base) if r.doc["status"] == "paid"]
    for j in sample_idx(len(paid_idx), 0.008, rng):
        r = base[paid_idx[j]]
        d = copy.deepcopy(r.doc)
        d["paid_at"] = None
        d["updated_at"] = bump(d["updated_at"])
        extra.append(Rec(r.day, d["updated_at"], r.key, d))
        mf(r.day, "invoices", "kafka", r.key, "paid_without_paid_at", "paid_at", None, "quarantine")
    for i in sample_idx(len(base), 0.008, rng):
        r = base[i]
        d = copy.deepcopy(r.doc)
        d["status"] = ["PAID", "settled"][int(rng.integers(0, 2))]
        d["updated_at"] = bump(d["updated_at"])
        extra.append(Rec(r.day, d["updated_at"], r.key, d))
        mf(r.day, "invoices", "kafka", r.key, "unknown_status", "status", d["status"], "quarantine")
    extra += malformed(base, "invoices", "kafka", 0.003)
    extra += dups(base, "invoices", "kafka", 0.006, 0.006)
    invoices += extra

    # ---- usage_events (JSON, Kafka)
    base, extra = list(usage), []
    counter = [0]

    def new_event_id():
        counter[0] += 1
        return f"E9{counter[0]:06d}"

    for i in sample_idx(len(base), 0.001, rng):
        r = base[i]
        d = dict(r.doc, event_id=new_event_id())
        d["event_ts"] = ["yesterday", "2025-02-30T10:00:00"][int(rng.integers(0, 2))]
        extra.append(Rec(r.day, r.ts, r.key, d))
        mf(r.day, "usage_events", "kafka", d["event_id"], "wrong_type", "event_ts", d["event_ts"], "rescued_data")
    for i in sample_idx(len(base), 0.0015, rng):
        r = base[i]
        d = dict(r.doc, event_id=new_event_id(), user_id=None)
        extra.append(Rec(r.day, r.ts, r.key, d))
        mf(r.day, "usage_events", "kafka", d["event_id"], "missing_value", "user_id", None, "null_handling")
    for i in sample_idx(len(base), 0.001, rng):
        r = base[i]
        d = dict(r.doc, event_id=new_event_id(), customer_id="C9999")
        extra.append(Rec(r.day, r.ts, "C9999", d))
        mf(r.day, "usage_events", "kafka", d["event_id"], "unknown_customer", "customer_id", "C9999", "quarantine")
    extra += malformed(base, "usage_events", "kafka", 0.0003)
    extra += dups(base, "usage_events", "kafka", 0.0015, 0.0015)
    usage += extra

    # ---- tickets (JSON, Kafka)
    base, extra = list(tickets), []
    counter = [0]

    def new_ticket_id():
        counter[0] += 1
        return f"T9{counter[0]:05d}"

    for i in sample_idx(len(base), 0.012, rng):
        r = base[i]
        d = dict(r.doc, ticket_id=new_ticket_id())
        d["created_at"] = ["n/a", "31/12/2025 10:00"][int(rng.integers(0, 2))]
        extra.append(Rec(r.day, r.ts, r.key, d))
        mf(r.day, "tickets", "kafka", d["ticket_id"], "wrong_type", "created_at", d["created_at"], "rescued_data")
    for i in sample_idx(len(base), 0.012, rng):
        r = base[i]
        if rng.random() < 0.5:
            d = dict(r.doc, ticket_id=new_ticket_id(), priority="URGENT")
            handling = "quarantine"
        else:
            d = dict(r.doc, priority=r.doc["priority"].upper() + " ")
            handling = "trim_normalize"
        extra.append(Rec(r.day, r.ts, r.key, d))
        mf(r.day, "tickets", "kafka", d["ticket_id"], "unknown_or_case_priority", "priority", d["priority"],
           handling)
    closed_idx = [i for i, r in enumerate(base) if r.doc["closed_at"]]
    for j in sample_idx(len(closed_idx), 0.012, rng):
        r = base[closed_idx[j]]
        d = dict(r.doc, ticket_id=new_ticket_id(), created_at=r.doc["closed_at"], closed_at=r.doc["created_at"])
        extra.append(Rec(r.day, r.ts, r.key, d))
        mf(r.day, "tickets", "kafka", d["ticket_id"], "closed_before_created", "closed_at", d["closed_at"],
           "quarantine")
    for i in sample_idx(len(base), 0.006, rng):
        r = base[i]
        d = dict(r.doc, ticket_id=new_ticket_id(), customer_id=None)
        extra.append(Rec(r.day, r.ts, None, d))
        mf(r.day, "tickets", "kafka", d["ticket_id"], "null_key", "customer_id", None, "quarantine")
    extra += dups(base, "tickets", "kafka", 0.006, 0.006)
    tickets += extra

    manifest.sort(key=lambda m: (m[0], m[1], m[3]))
    return manifest


# ----------------------------------------------------------------------------- zapis
def group_by_day(recs):
    out = {}
    for r in recs:
        out.setdefault(r.day, []).append(r)
    for lst in out.values():
        lst.sort(key=lambda r: r.ts)
    return out


def write_all(customers, subs, invoices, usage, tickets, manifest, gt_rows):
    for sub in ("files", "kafka"):
        shutil.rmtree(OUT / sub, ignore_errors=True)
    (OUT / "drop_state.json").unlink(missing_ok=True)

    (OUT / "files" / "plans").mkdir(parents=True, exist_ok=True)
    with open(OUT / "files" / "plans" / "plans.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["plan_id", "plan_name", "monthly_price", "seats_included", "extra_seat_price"])
        w.writerows(PLANS)

    cdir = OUT / "files" / "customers"
    cdir.mkdir(parents=True, exist_ok=True)
    for day, recs in group_by_day(customers).items():
        with open(cdir / f"customers_{day.isoformat()}.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CUSTOMER_FIELDS)
            w.writeheader()
            for r in recs:
                w.writerow(r.doc)

    counts = {}
    for topic, recs in (("subscriptions", subs), ("invoices", invoices), ("usage_events", usage),
                        ("tickets", tickets)):
        tdir = OUT / "kafka" / topic
        tdir.mkdir(parents=True, exist_ok=True)
        grouped = group_by_day(recs)
        counts[topic] = (len(recs), len(grouped))
        for day, lst in grouped.items():
            (tdir / f"{topic}_{day.isoformat()}.jsonl").write_text(
                "\n".join(r.line() for r in lst) + "\n", encoding="utf-8")

    with open(OUT / "dirty_manifest.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["day", "entity", "channel", "key", "kind", "column", "bad_value", "expected_handling"])
        w.writerows(manifest)
    with open(OUT / "ground_truth_mrr.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["month", "mrr", "new_mrr", "expansion_mrr", "contraction_mrr", "churned_mrr",
                    "churned_mrr_payment_failure", "active_customers"])
        w.writerows(gt_rows)
    return counts


def main():
    ap = argparse.ArgumentParser(description="Generator danych SaaS v2")
    ap.add_argument("--customers", type=int, default=500, help="liczba klientów (domyślnie 500)")
    ap.add_argument("--end", default=DEFAULT_END.isoformat(), help="ostatni dzień symulacji (domyślnie 2026-12-31)")
    ap.add_argument("--clean", action="store_true", help="bez celowo brudnych danych")
    args = ap.parse_args()
    end = date.fromisoformat(args.end)

    rng = np.random.default_rng(SEED)
    Faker.seed(SEED)
    fake = Faker("en_GB")

    customers, subs, invoices, usage, tickets, gt_rows, stats = simulate(args.customers, end, rng, fake)
    n_clean = {"customers": len(customers), "subscriptions": len(subs), "invoices": len(invoices),
               "usage_events": len(usage), "tickets": len(tickets)}
    manifest = [] if args.clean else inject_dirty(customers, subs, invoices, usage, tickets, rng, end)
    counts = write_all(customers, subs, invoices, usage, tickets, manifest, gt_rows)

    print(f"Okres: {SIM_START} .. {end}  (backfill do {BACKFILL_END}, potem dzień po dniu)")
    print(f"{'zbiór':<15}{'wiersze':>9}{'(czyste)':>10}{'dni z plikiem':>15}")
    n_cust_days = len(list((OUT / 'files' / 'customers').glob('*.csv')))
    print(f"{'customers':<15}{len(customers):>9}{n_clean['customers']:>10}{n_cust_days:>15}")
    for topic, (n, d) in counts.items():
        print(f"{topic:<15}{n:>9}{n_clean[topic]:>10}{d:>15}")
    print(f"\nrezygnacje: dobrowolne {stats['churn_voluntary']}, przymusowe (payment_failure) "
          f"{stats['churn_payment_failure']}")
    print(f"podłożone usterki: {len(manifest)} (dirty_manifest.csv)")
    print(f"\nPliki zapisane w: {OUT.resolve()}")
    print("UWAGA: stan wysyłki (drop_state.json) został wyzerowany. Wiadomości już wysłane do Kafki zostają w topikach.")


if __name__ == "__main__":
    main()
