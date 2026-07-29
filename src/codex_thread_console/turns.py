from __future__ import annotations

from typing import Any, Mapping, TypedDict


class TurnOptions(TypedDict, total=False):
    """Stable official-SDK turn overrides exposed by the control plane."""

    cwd: str
    effort: str
    model: str
    output_schema: dict[str, Any]
    personality: str
    service_tier: str
    summary: str


_OPTION_KEYS = frozenset(TurnOptions.__annotations__)
_PERSONALITIES = {"none", "friendly", "pragmatic"}
_SUMMARIES = {"none", "auto", "concise", "detailed"}


def normalize_turn_options(
    options: Mapping[str, Any] | None,
) -> TurnOptions:
    if not options:
        return {}
    unknown = set(options) - _OPTION_KEYS
    if unknown:
        names = ", ".join(sorted(unknown))
        raise ValueError(f"unsupported turn option(s): {names}")

    normalized: TurnOptions = {}
    for key in ("cwd", "effort", "model", "service_tier"):
        value = options.get(key)
        if value is None:
            continue
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{key} must be a non-empty string")
        normalized[key] = value.strip()  # type: ignore[literal-required]

    personality = options.get("personality")
    if personality is not None:
        if personality not in _PERSONALITIES:
            raise ValueError(
                "personality must be one of: friendly, none, pragmatic"
            )
        normalized["personality"] = personality

    summary = options.get("summary")
    if summary is not None:
        if summary not in _SUMMARIES:
            raise ValueError(
                "summary must be one of: auto, concise, detailed, none"
            )
        normalized["summary"] = summary

    output_schema = options.get("output_schema")
    if output_schema is not None:
        if not isinstance(output_schema, dict):
            raise ValueError("output_schema must be a JSON object")
        normalized["output_schema"] = dict(output_schema)
    return normalized
