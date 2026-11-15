"""Generate repeatable canonical IDs from the source key that creates an entity."""

import uuid

NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")


def generate_canonical_id(source_system, source_id):
    source_key = f"{source_system.strip().casefold()}:{source_id.strip().casefold()}"
    identifier = uuid.uuid5(NAMESPACE, source_key)
    return f"CAN-{identifier.hex[:16].upper()}"
