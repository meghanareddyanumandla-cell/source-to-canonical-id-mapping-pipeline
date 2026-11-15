"""Validate customer records and quarantine duplicate source keys."""

import re


def _phone_digits(value):
    return "".join(character for character in (value or "") if character.isdigit())


def validate_record(record):
    """Return issue codes for one record; an empty list means it is valid."""
    issues = []
    for field in ("source_system", "source_id", "name"):
        if not (record.get(field) or "").strip():
            issues.append(f"missing_required:{field}")

    email = (record.get("email") or "").strip()
    if email and not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email):
        issues.append("invalid_email")

    phone = (record.get("phone") or "").strip()
    if phone:
        digits = _phone_digits(phone)
        if not 7 <= len(digits) <= 15 or not re.fullmatch(r"[+()\d .-]+", phone):
            issues.append("invalid_phone")

    age = (record.get("age") or "").strip()
    if age:
        try:
            number = int(age)
            if str(number) != age or not 0 <= number <= 120:
                issues.append("invalid_age")
        except ValueError:
            issues.append("invalid_age")

    return issues


def validate_records(rows):
    """Return valid records and rejects, including every duplicate-key row."""
    valid = []
    rejects = []
    duplicate_keys = set()
    key_counts = {}

    for row in rows:
        issues = validate_record(row)
        key = (
            (row.get("source_system") or "").casefold(),
            (row.get("source_id") or "").casefold(),
        )
        if all(key):
            key_counts[key] = key_counts.get(key, 0) + 1
            if key_counts[key] > 1:
                duplicate_keys.add(key)
        if issues:
            rejects.append({**row, "issues": issues})
        else:
            valid.append(row)

    if duplicate_keys:
        unique_rows = []
        for row in valid:
            key = (row["source_system"].casefold(), row["source_id"].casefold())
            if key in duplicate_keys:
                rejects.append({**row, "issues": ["duplicate_source_key_in_batch"]})
            else:
                unique_rows.append(row)
        valid = unique_rows

    return valid, rejects
