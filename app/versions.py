"""Model-version constants, lenient input parsing and vocabulary metadata.

Two classifier generations share one simulated node:

* ``v1`` is the **original** model / vocabulary.  It is the default both for
  new submissions that omit a version and for pre-upgrade SQLite rows migrated
  with ``DEFAULT 'v1'``.
* ``v2`` is the **new** model with a different simulated vocabulary.

The canonical wire/storage token is ``"v1"`` / ``"v2"``.  Parsing is
deliberately lenient (``original`` / ``new`` / ``old`` / ... are accepted)
because callers and docs may use either naming; everything persisted and
scheduled uses the canonical token.
"""

from __future__ import annotations

from typing import Final

MODEL_VERSION_V1: Final[str] = "v1"
MODEL_VERSION_V2: Final[str] = "v2"
DEFAULT_MODEL_VERSION: Final[str] = MODEL_VERSION_V1

#: Canonical versions in switch/fallback order.
MODEL_VERSIONS: Final[tuple[str, str]] = (MODEL_VERSION_V1, MODEL_VERSION_V2)

# Human / Swagger labels for the two generations.
MODEL_VERSION_LABELS: Final[dict[str, str]] = {
    MODEL_VERSION_V1: "original",
    MODEL_VERSION_V2: "new",
}

# Lenient request aliases -> canonical token.  Keys are lower/stripped.
_VERSION_ALIASES: Final[dict[str, str]] = {
    "v1": MODEL_VERSION_V1,
    "1": MODEL_VERSION_V1,
    "ver1": MODEL_VERSION_V1,
    "version1": MODEL_VERSION_V1,
    "original": MODEL_VERSION_V1,
    "orig": MODEL_VERSION_V1,
    "old": MODEL_VERSION_V1,
    "classic": MODEL_VERSION_V1,
    "baseline": MODEL_VERSION_V1,
    "base": MODEL_VERSION_V1,
    "legacy": MODEL_VERSION_V1,
    "原版": MODEL_VERSION_V1,
    "旧版": MODEL_VERSION_V1,
    "老版": MODEL_VERSION_V1,
    "v2": MODEL_VERSION_V2,
    "2": MODEL_VERSION_V2,
    "ver2": MODEL_VERSION_V2,
    "version2": MODEL_VERSION_V2,
    "new": MODEL_VERSION_V2,
    "newest": MODEL_VERSION_V2,
    "latest": MODEL_VERSION_V2,
    "next": MODEL_VERSION_V2,
    "modern": MODEL_VERSION_V2,
    "新版": MODEL_VERSION_V2,
    "新版本": MODEL_VERSION_V2,
}

# Request-body keys that may carry the version choice.  ``model_version`` is
# canonical; the others are accepted as aliases so existing/alternative
# clients keep working.
VERSION_FIELD_ALIASES: Final[tuple[str, ...]] = (
    "model_version",
    "version",
    "model",
    "model_name",
    "model_variant",
    "variant",
    "modelVersion",
    "model_generation",
    "generation",
)

# Alternative "use the new model?" boolean selectors some clients may use.
NEW_MODEL_BOOL_KEYS: Final[tuple[str, ...]] = (
    "use_new_model",
    "new_model",
    "use_new_version",
    "new_version",
    "is_new_version",
    "is_new_model",
    "use_v2",
    "enable_v2",
)


class UnknownModelVersion(ValueError):
    """Raised when a caller supplied a version token that cannot be parsed."""


def normalize_model_version(value: object) -> str:
    """Return the canonical ``"v1"``/``"v2"`` token for many input forms.

    Empty / ``None`` means "omitted" and resolves to the original ``v1``.
    Raises ``UnknownModelVersion`` for non-empty tokens we do not recognise so
    the HTTP layer can answer 422.
    """

    if value is None:
        return DEFAULT_MODEL_VERSION
    if not isinstance(value, str):
        # Be tolerant with numbers / enums.
        value = str(value)
    token = value.strip().lower()
    if not token:
        return DEFAULT_MODEL_VERSION
    if token in _VERSION_ALIASES:
        return _VERSION_ALIASES[token]
    # Maybe an enum-like "ModelVersion.V2" or "Literal['v2']" repr.
    if "." in token:
        tail = token.rsplit(".", 1)[-1]
        if tail in _VERSION_ALIASES:
            return _VERSION_ALIASES[tail]
    raise UnknownModelVersion(
        f"未知模型版本: {value!r}；可选 {MODEL_VERSIONS} "
        f"（或别名 original/new、原版/新版）"
    )


def version_label(version: str | None) -> str:
    """Stable human label (``original`` / ``new``) for a canonical token."""

    return MODEL_VERSION_LABELS.get(
        normalize_model_version(version), "original"
    )


def _is_truthy_token(value: object) -> bool | None:
    """Interpret an optional boolean-ish selector.

    Returns True/False for recognisable values and None when the value does
    not look like a boolean selector.
    """

    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value == 2:  # generation=2
            return True
        if value == 1:
            return False
        return bool(value)
    if isinstance(value, str):
        token = value.strip().lower()
        if token in ("true", "1", "yes", "y", "on", "v2", "2", "new"):
            return True
        if token in ("false", "0", "no", "n", "off", "v1", "old"):
            return False
    return None


def version_from_mapping(data: object) -> object | None:
    """Best-effort version selector extracted from a raw request mapping.

    Looks first for explicit version-token keys, then for boolean
    "use_new_model" style keys.  Returns None when the caller expressed no
    preference.
    """

    if not isinstance(data, dict):
        return None
    for key in VERSION_FIELD_ALIASES:
        value = data.get(key)
        if value not in (None, ""):
            return value
    for key in NEW_MODEL_BOOL_KEYS:
        if key in data and data[key] is not None:
            flag = _is_truthy_token(data[key])
            if flag is not None:
                return MODEL_VERSION_V2 if flag else MODEL_VERSION_V1
    return None
