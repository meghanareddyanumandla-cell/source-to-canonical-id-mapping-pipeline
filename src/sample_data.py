"""Generate paired, deterministic customer feeds for local runs."""

import argparse
import csv
import random
from pathlib import Path

FIRST_NAMES = (
    "Aanya", "Aarav", "Aisha", "Akash", "Amara", "Arjun", "Anika",
    "Daniel", "David", "Elena", "Emma", "Ethan", "Fatima", "Grace",
    "Hannah", "Ibrahim", "Isha", "Jacob", "James", "Kavya", "Leo",
    "Liam", "Maya", "Mia", "Noah", "Olivia", "Priya", "Ravi",
    "Ryan", "Sara", "Sophia", "Vikram", "Zara",
)
LAST_NAMES = (
    "Adams", "Brown", "Chen", "Clark", "Davis", "Garcia", "Gupta",
    "Harris", "Iyer", "Jackson", "Johnson", "Khan", "Kim", "Kumar",
    "Lee", "Lewis", "Martin", "Mehta", "Miller", "Patel", "Reddy",
    "Robinson", "Shah", "Sharma", "Singh", "Taylor", "Thomas",
    "Walker", "Wang", "Wilson", "Young",
)

FIELDS = (
    "source_system",
    "source_id",
    "name",
    "email",
    "phone",
    "address",
    "age",
)


def _write_source(path, source, rows, seed):
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=FIELDS)
        writer.writeheader()
        for number in range(rows):
            # Recreate the same customer details for both supplier files.
            randomizer = random.Random(seed + number)
            first_name = randomizer.choice(FIRST_NAMES)
            last_name = randomizer.choice(LAST_NAMES)
            email_name = f"{first_name}.{last_name}{number:06d}".casefold()
            writer.writerow(
                {
                    "source_system": source,
                    "source_id": f"{source.upper()}{number:07d}",
                    "name": f"{first_name} {last_name}",
                    "email": f"{email_name}@example.test",
                    "phone": f"+1-202-555-{number:06d}",
                    "address": f"{randomizer.randint(1, 999)} Example Street",
                    "age": randomizer.randint(18, 90),
                }
            )


def _append_edge_cases(path):
    cases = [
        {
            "source_system": "a",
            "source_id": "INVALID-MISSING-NAME",
            "name": "",
            "email": "bad-email",
            "phone": "12",
            "address": "",
            "age": "abc",
        },
        {
            "source_system": "a",
            "source_id": "A0000000",
            "name": "Duplicate key",
            "email": "duplicate@example.test",
            "phone": "+1 202 555 0101",
            "address": "",
            "age": "0",
        },
        {
            "source_system": "a",
            "source_id": "NO-CONTACT",
            "name": "No Contact",
            "email": "",
            "phone": "",
            "address": "",
            "age": "120",
        },
    ]
    with path.open("a", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=FIELDS)
        writer.writerows(cases)


def generate(output_dir, rows=12, seed=17, include_edge_cases=True):
    """Write two feeds with shared customer details and optional bad rows."""
    if rows < 1:
        raise ValueError("rows must be at least 1")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = []

    for source in ("a", "b"):
        path = output_dir / f"source_{source}.csv"
        _write_source(path, source, rows, seed)
        paths.append(str(path))

    if include_edge_cases:
        _append_edge_cases(output_dir / "source_a.csv")

    return paths


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="data")
    parser.add_argument("--rows", type=int, default=12)
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--no-edge-cases", action="store_true")
    args = parser.parse_args()

    paths = generate(args.output, args.rows, args.seed, not args.no_edge_cases)
    print("\n".join(paths))


if __name__ == "__main__":
    main()
