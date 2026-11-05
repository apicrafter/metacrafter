"""Credential redaction and explicit sample-disclosure policies."""

import re


def redact(value):
    if isinstance(value, dict):
        return {
            key: (
                "[REDACTED]"
                if any(
                    word in key.lower()
                    for word in (
                        "password",
                        "secret",
                        "token",
                        "api_key",
                        "authorization",
                    )
                )
                else redact(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    text = str(value)
    text = re.sub(r"(://[^:/\s]+:)[^@/\s]+@", r"\1[REDACTED]@", text)
    return re.sub(
        r"(?i)((?:api[_-]?key|password|token)\s*[=:]\s*)[^,;\s]+", r"\1[REDACTED]", text
    )


def disclose_samples(values, policy="names-only", limit=5):
    if policy not in ("names-only", "redacted", "raw"):
        raise ValueError("sample_policy must be names-only, redacted or raw")
    if policy == "names-only":
        return []
    values = [str(value)[:256] for value in values if value is not None][:limit]
    if policy == "redacted":
        return [
            "".join(
                "0" if ch.isdigit() else "x" if ch.isalpha() else ch for ch in value
            )
            for value in values
        ]
    return values
