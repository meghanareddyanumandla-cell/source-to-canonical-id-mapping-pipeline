"""Match normalized email and phone values against canonical entities."""


def normalize_email(value):
    return (value or "").strip().casefold()


def normalize_phone(value):
    return "".join(character for character in (value or "") if character.isdigit())


def build_index(entities):
    """Build lookups so resolving a row does not scan the full catalog."""
    index = {"email": {}, "phone": {}, "entities": {}}
    for entity in entities:
        index_entity(index, entity)
    return index


def index_entity(index, entity):
    canonical_id = entity["canonical_id"]
    index["entities"][canonical_id] = entity

    email = normalize_email(entity.get("email"))
    if email:
        index["email"].setdefault(email, set()).add(canonical_id)

    phone = normalize_phone(entity.get("phone"))
    if phone:
        index["phone"].setdefault(phone, set()).add(canonical_id)


def find_match(record, entities, index=None):
    """Return (status, canonical_id, reason) for one validated source row."""
    if index is None:
        index = build_index(entities)

    email = normalize_email(record.get("email"))
    phone = normalize_phone(record.get("phone"))
    if not email and not phone:
        return "unresolved", None, "no_match_identifiers"

    email_matches = index["email"].get(email, set()) if email else set()
    phone_matches = index["phone"].get(phone, set()) if phone else set()
    matches = email_matches | phone_matches

    if len(matches) > 1:
        if len(email_matches) > 1 or len(phone_matches) > 1:
            reason = "multiple_entities_share_an_identifier"
        else:
            reason = "email_and_phone_point_to_different_entities"
        return "conflict", None, reason

    if not matches:
        return "unresolved", None, "no_exact_match"

    canonical_id = next(iter(matches))
    entity = index["entities"][canonical_id]
    old_email = normalize_email(entity.get("email"))
    old_phone = normalize_phone(entity.get("phone"))
    if (email and old_email and email != old_email) or (
        phone and old_phone and phone != old_phone
    ):
        return "conflict", None, "matched_identifier_has_conflicting_companion_identifier"

    return "matched", canonical_id, "exact_identifier_match"
