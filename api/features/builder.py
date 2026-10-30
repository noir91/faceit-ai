"""Convert one FACEIT API result into one match-level feature row."""

import re


WINDOWS = ("15d", "30d")
MAPS = (
    "ancient", "anubis", "cache", "dust2", "inferno",
    "mirage", "nuke", "overpass", "vertigo", "train",
)
BASE = (
    "ADR", "Kills", "Quadro Kills", "Deaths", "Headshots",
    "MVPs", "Double Kills", "Penta Kills", "Headshots %",
    "Triple Kills", "K/D Ratio", "win_ratio", "Assists",
    "Damage", "K/R Ratio",
)

KNOWN_KEYS = (
    *BASE,
    *(f"de_{name}_count" for name in MAPS),
    *(f"de_{name}_wr" for name in MAPS),
)
ALL_KEYS = {*KNOWN_KEYS, "other_map_count", "other_map_wr"}
KNOWN_MAPS = {f"de_{name}" for name in MAPS}


def clean(name: str) -> str:
    """Use the same feature-name normalization as training."""
    return re.sub(r"[^0-9a-zA-Z]+", "_", name.replace("%", "pct")).strip("_").lower()


def flatten_player(player_stats: dict) -> dict:
    """Transform one player's two windows into training-style feature names."""
    features = {}

    for window in WINDOWS:
        stats = player_stats.get(f"matches_{window}") or {}
        prefix = f"{window}__"

        # Original fixed feature set. Missing statistics stay null, not zero.
        for key in KNOWN_KEYS:
            value = stats.get(key)
            features[prefix + clean(key)] = None if value is None else float(value)

        # Combine counts and win rates of maps outside the training map list.
        other_counts = {
            key[:-6]: float(value or 0)
            for key, value in stats.items()
            if key not in ALL_KEYS and key.endswith("_count")
            and key[:-6] not in KNOWN_MAPS
        }
        other_wrs = {
            key[:-3]: float(value or 0)
            for key, value in stats.items()
            if key not in ALL_KEYS and key.endswith("_wr")
            and key[:-3] not in KNOWN_MAPS
        }

        weighted = [
            (count, other_wrs[name])
            for name, count in other_counts.items()
            if count > 0 and name in other_wrs
        ]
        total_weight = sum(count for count, _ in weighted)

        features[prefix + "other_map_count"] = sum(other_counts.values())
        features[prefix + "other_map_wr"] = (
            sum(count * win_rate for count, win_rate in weighted) / total_weight
            if total_weight else None
        )

    # Training adds n_matches after flattening BOTH windows.
    # It sums known-map counts only (not other_map_count).
    for window in WINDOWS:
        stats = player_stats.get(f"matches_{window}") or {}
        features[f"{window}__n_matches"] = sum(
            float(stats.get(f"de_{name}_count") or 0) for name in MAPS
        )

    return features


def build_match_row(data: dict, *, include_label: bool = False) -> dict:
    """One FACEIT response -> one wide row (p1...p10)."""
    result = data.get("result", data)  # Accept full API response or result alone.
    match = result["matches_elo"]
    teams = match["teams"]

    # Preserve training slot order: faction1 players first, then faction2.
    roster = teams["faction1"]["roster"] + teams["faction2"]["roster"]
    player_ids = [player["player_id"] for player in roster]

    if len(player_ids) != 10 or len(set(player_ids)) != 10:
        raise ValueError("Expected exactly 10 distinct players (5 per faction).")

    recent = result["recentprf"]
    by_player = {entry["player_ids"]: entry for entry in recent}
    if len(by_player) != len(recent):
        raise ValueError("Duplicate player statistics in recentprf.")

    missing = [player_id for player_id in player_ids if player_id not in by_player]
    if missing:
        raise ValueError(f"Missing recent statistics for {len(missing)} player(s).")

    picks = match["voting"]["map"]["pick"]
    if len(picks) != 1:
        raise ValueError("Expected exactly one picked map.")

    rating_1 = teams["faction1"]["stats"].get("rating")
    rating_2 = teams["faction2"]["stats"].get("rating")

    # Match-level columns, same order as training.
    row = {
        "match_id": match["match_id"],
        "started_at": match["started_at"],
        "map": picks[0],
        "rating_1": rating_1,
        "rating_2": rating_2,
    }
    if include_label:  # Only for historical validation, NEVER model inputs.
        row["label"] = int(match["results"]["winner"] == "faction1")

    row["rating_diff"] = (
        rating_1 - rating_2 if rating_1 is not None and rating_2 is not None else None
    )

    # Prefix each player's features with its roster slot: p1_ ... p10_.
    for slot, player_id in enumerate(player_ids, start=1):
        features = flatten_player(by_player[player_id])
        row.update({f"p{slot}_{name}": value for name, value in features.items()})

    row["n_slots"] = 10  # All ten player-stat records were found.
    return row
