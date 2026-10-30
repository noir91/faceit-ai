import time
import polars as pl
import asyncio


async def fetch_match_inputs(match_id, client, session, recent_semaphore):
    """Return matches_elo plus one recentprf anchor for each of the ten players.

    Pass your existing FaceitClient, shared aiohttp ClientSession, and a shared
    asyncio.Semaphore. This function does not write to MongoDB or expand seeds.
    """
    request_time = int(time.time())

    # Same match-details transformation used by the ingestion pipeline.
    elo = await client.matches_elo(match_id, session, count=None)
    if not isinstance(elo, dict) or not elo:
        raise RuntimeError(f"Could not fetch match details: {match_id}")

    rosters = [
        elo.get("teams", {}).get(faction, {}).get("roster", [])
        for faction in ("faction1", "faction2")
    ]
    if any(len(roster) != 5 for roster in rosters):
        raise ValueError("Expected five players in each faction.")

    # Keep faction1's five players first, followed by faction2's five players.
    player_ids = [player["player_id"] for roster in rosters for player in roster]
    if len(set(player_ids)) != 10:
        raise ValueError("Expected ten distinct player IDs.")

    # Historical match: stop at its start. Upcoming match: stop at request time.
    started_at = int(elo.get("started_at") or 0)
    anchor_time = min(started_at, request_time) if started_at > 0 else request_time

    # Same input shape produced by master_function's group_by aggregation,
    # with exactly one anchor match per player.
    master_fetch = [
        {
            "player_ids": player_id,
            "match_ids": [{
                "_id": match_id,
                "started_at": anchor_time,
                "fifteen_window": anchor_time - 1296000,
                "thirty_window": anchor_time - 2592000,
            }],
            "max_started_at": anchor_time,
            "min_started_at": anchor_time - 2592000,
        }
        for player_id in player_ids
    ]

    async def worker(data):
        async with recent_semaphore:
            # Same three steps as the worker inside your master_function().
            match_df = (
                pl.DataFrame(data)
                .unnest("match_ids")
                .with_columns(
                    (pl.col("started_at") - 1296000).alias("fifteen_window"),
                    (pl.col("started_at") - 2592000).alias("thirty_window"),
                )
                .rename({"started_at": "original_window"})
            )

            fetched = await client.recent_performance(data, session)
            aggregated = client.aggregate_statistics(fetched, match_df, session)
            if len(aggregated) != 1:
                raise RuntimeError(
                    f"Expected one recent-performance anchor for {data['player_ids']}"
                )
            return aggregated[0]

    recentprf = await asyncio.gather(
        *(worker(data) for data in master_fetch),
        return_exceptions=True,
    )
    # Finish the workers, then propagate any error; never return a partial team.
    for result in recentprf:
        if isinstance(result, BaseException):
            raise result

    return {
        "match_id": match_id,
        "started_at": anchor_time,
        "matches_elo": elo,
        "recentprf": recentprf,
    }

        


