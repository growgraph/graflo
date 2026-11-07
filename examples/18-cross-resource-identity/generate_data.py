"""
Regenerate the two CSV fixtures for example 18.

Committed output is deterministic (fixed seed), so the discovery result is
stable across runs. Only needed if you want to change the shape of the data:

    cd examples/18-cross-resource-identity
    uv run python generate_data.py
"""

from __future__ import annotations

import csv
import random
from pathlib import Path

import click

DATA_DIR = Path(__file__).resolve().parent / "data"
N_CUSTOMERS = 150
SEED = 20260731

COUNTRIES = ["GB", "US", "DE", "FR", "NL"]
FIRST = ["ada", "grace", "alan", "edsger", "barbara", "donald", "john", "tony"]
LAST = [
    "lovelace",
    "hopper",
    "turing",
    "dijkstra",
    "liskov",
    "knuth",
    "backus",
    "hoare",
]


def main() -> None:
    rng = random.Random(SEED)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    crm_rows = []
    billing_rows = []
    for i in range(N_CUSTOMERS):
        first = FIRST[i % len(FIRST)]
        last = LAST[(i // len(FIRST)) % len(LAST)]
        email = f"{first}.{last}{i}@example.com"
        country = COUNTRIES[i % len(COUNTRIES)]
        phone = f"+{rng.randint(1, 99)}{rng.randint(10**8, 10**9 - 1)}"

        crm_rows.append(
            {
                "customer_email": email.upper() if i % 7 == 0 else email,
                "full_name": f"{first.title()} {last.title()}",
                "signup_country": country,
            }
        )
        # Same people, different column names, plus formatting drift on the
        # shared key — which is exactly what `normalize_for_match` absorbs.
        billing_rows.append(
            {
                "email_address": f"  {email} " if i % 5 == 0 else email,
                "phone": phone,
                "country": country,
                "invoice_total": round(rng.uniform(10, 5000), 2),
            }
        )

    _write(DATA_DIR / "crm_customers.csv", crm_rows)
    _write(DATA_DIR / "billing_accounts.csv", billing_rows)
    click.echo(
        f"Wrote {N_CUSTOMERS} rows to each of crm_customers.csv, billing_accounts.csv"
    )


def _write(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
