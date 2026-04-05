"""
Installer invitation secrets: 16 random bytes → Base58 for employees, SHA-256 hex in DB.

Legacy rows stored plaintext `token` in MySQL; migration sets
`token_hash = sha256(utf8(old_token)).hexdigest()` so old links keep working.

New invitations also persist Base58 in `installer_token` so manager list API can
re-show the secret (SaaS); rows with only `token_hash` keep `installer_token` null.
"""

from __future__ import annotations

import hashlib
import secrets
from typing import Optional, Tuple

import base58

RAW_BYTE_LEN = 16


def generate_invitation_secret() -> Tuple[bytes, str, str]:
    """Return (raw_16_bytes, base58_for_employee, sha256_hex_for_db)."""
    raw = secrets.token_bytes(RAW_BYTE_LEN)
    display = base58.b58encode(raw).decode("ascii")
    digest = hashlib.sha256(raw).hexdigest()
    return raw, display, digest


def invitation_token_hash_from_client(client_token: str) -> Optional[str]:
    """
    Map user input to stored `token_hash` (64 lowercase hex chars).

    * New tokens: Base58 decodes to exactly 16 bytes → SHA-256(raw).
    * Legacy: any other non-empty string → SHA-256(UTF-8 bytes) (matches migrated DB rows).
    """
    s = (client_token or "").strip()
    if not s:
        return None
    try:
        raw = base58.b58decode(s)
        if len(raw) == RAW_BYTE_LEN:
            return hashlib.sha256(raw).hexdigest()
    except Exception:
        pass
    return hashlib.sha256(s.encode("utf-8")).hexdigest()
