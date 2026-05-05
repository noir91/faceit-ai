# Pipeline — Ingestion & Orchestration

## What this is

This pipeline collects raw match data from the Faceit Data API and builds a graph-based dataset of CS2 players for downstream skill modelling. It follows a two-stage snowball sampling approach — starting from a small set of seed players, expanding outward through their co-players (alters), and collecting statistics at every level.

The final dataset feeds an ML pipeline for player skill estimation and ranking. The output lives entirely in MongoDB across five collections that together form the full data contract.

---

# System Overview
![Zoom In](pipeline/imgs/pwf.jpeg)

## How to run it

The pipeline is run manually. Before starting, ensure MongoDB is running and the `players` collection has been seeded (see [Seeding Players](#seeding-players) below).

```python
from pipeline.runner import PipelineRunner

runner = PipelineRunner(
    headers={"Authorization": "Bearer <your_api_key>"},
    host="localhost",
    port=27017
)

asyncio.run(runner.supermatch())
```

The pipeline will prompt for confirmation before executing:

```
You want pipeline to execute? (Y/N):
```

If a previous run was interrupted, it resumes automatically from the last checkpoint. If no checkpoint exists, it starts fresh from the `players` collection.

---

## Seeding Players

Before the first run, the `players` collection must be populated. There are two ways to do this:

**From a Faceit hub** — fetches all hub members and saves them to MongoDB:
```python
client = FaceitClient(dbobj=data, headers=headers)
members = client.retrieve_hub_members(hub_id="<your_hub_id>")
```

**From a nickname list** — resolves usernames to player IDs:
```python
ids = client.retrieve_ID_members(nicknames=["player1", "player2"])
```

---

## System Overview

```
                    ┌─────────────────┐
                    │  Seed Players   │  ← players collection (manual seed)
                    └────────┬────────┘
                             │
                    ╔════════▼════════╗
                    ║    Stage  1     ║
                    ╠═════════════════╣
                    ║  match()        ║  fetch last 90d of matches (paginated)
                    ║  randomizer()   ║  sample N unique, non-duplicate matches
                    ║  alter_func()   ║  extract co-players (alters) per match
                    ║  stats_tform()  ║  compute faction aggregates per match
                    ║  matches_elo()  ║  strip + store match metadata + ELO
                    ║  lifetime()     ║  fetch per-map historical stats
                    ╚════════╤════════╝
                             │
               ┌─────────────┴──────────────┐
               │        MongoDB             │
               │  matches  ratings  alters  │
               │  matches_elo   lifetime    │
               └─────────────┬──────────────┘
                             │
                    ╔════════▼════════╗
                    ║    Stage  2     ║
                    ╠═════════════════╣
                    ║  collect_N()    ║  diff: stage_1 alters − stage_2 lifetime
                    ║  lifetime()     ║  fetch lifetime stats for alter players
                    ╚════════╤════════╝
                             │
               ┌─────────────┴──────────────┐
               │        MongoDB             │
               │       lifetime             │
               └────────────────────────────┘
```

---

## Data Contract

These are the five collections written by the pipeline and what they represent:

| Collection | Written by | What it stores |
|---|---|---|
| `matches` | `alter_function` | Raw match metadata for every sampled match — match ID, timestamps, playing players |
| `ratings` | `statistics_transform` | Per-match team aggregates and individual player stats. Primary input for the ML pipeline |
| `alters` | `alter_function` | Graph edges — each document is a match ID mapped to the list of player IDs who played in it |
| `matches_elo` | `matches_elo` | Stripped match metadata: ELO ratings, results, map pick, roster skill levels |
| `lifetime` | `lifetime_aggregates` | Historical per-map stats per player (win rate, avg performance per map). Written in both stages |

Every document carries a `stageId` field (`stage_1_YYYYMMDD_HHMM` or `stage_2_...`) which records which pipeline run produced it. This is used for checkpointing, deduplication across stages, and tracing data lineage.

---

## Modules

```
pipeline/
├── faceitclient.py   # Faceit API client — fetch, retry, transform
├── orch.py           # MongoDB connection and ingestion layer
└── runner.py         # Pipeline orchestration and batch processing
```

---

## Quick Reference

| Function | Module | What it does |
|---|---|---|
| `supermatch` | runner | Main entry point — runs the full pipeline |
| `helper_supermatch` | runner | Concurrent per-player processing loop |
| `batch_processor` | runner | Consumes queue, flushes batches to MongoDB |
| `checkpointer` | runner | Saves state on failure or interrupt |
| `most_recent_checkpoint` | runner | Finds unprocessed players to resume from |
| `backup_func` | runner | Backfills missing `stageId` across collections |
| `alter_function` | faceitclient | Fetches matches and extracts co-players |
| `match` | faceitclient | Fetches a player's recent match history |
| `match_randomizer` | faceitclient | Samples unique, non-duplicate match IDs |
| `statistics_transform` | faceitclient | Produces per-match faction aggregates for `ratings` |
| `matches_elo` | faceitclient | Fetches and strips match ELO metadata |
| `lifetime_aggregates` | faceitclient | Fetches per-map historical stats for a player |
| `collect_N` | faceitclient | Returns player IDs to process, stage-aware |
| `retrieve_hub_members` | faceitclient | Fetches all members of a Faceit hub |
| `retrieve_ID_members` | faceitclient | Resolves nicknames to player IDs |
| `call_api` | faceitclient | Central dispatcher for all API calls |
| `retry_function` | faceitclient | Retry wrapper with exponential backoff |
| `convert_json` | faceitclient | Aggregates player stats into team-level means |
| `connect_db` | orch | Connects to MongoDB and initialises collections |
| `store_data` | orch | Writes a batch of documents to a collection |

---

## `faceitclient.py` — `FaceitClient`

Handles all communication with the Faceit Data API. Every outbound request goes through this class.

---

### `call_api(url, endpoint, session, params=None, count=None)`

The central dispatcher for all outbound API calls. Every function that needs to hit the API calls this rather than making requests directly.

**Responsibility:** Routes the request to the correct fetch function, measures latency, checks for soft rate limiting, and logs the result.

**Endpoints handled:**

| `endpoint` value | Dispatches to | API path |
|---|---|---|
| `'statistics'` | `fetch_statistics_transform` | `matches/{id}/stats` |
| `'elo'` | `fetch_matches_elo` | `matches/{id}` |
| `'lifetime'` | `fetch_lifetime_url` | `players/{id}/stats/cs2` |
| `'match'` | `fetch_match` | `players/{id}/history` |

**Failure behaviour:** Passes each call through `retry_function` before returning. Detects silent rate limiting via `detect_soft_rate_limit` after the response arrives.

Returns `(data, status, latency)`.

#### Raw fetch functions

Each endpoint has a dedicated thin HTTP wrapper that performs a single `aiohttp` GET and returns `(response, data, status)`. They exist as separate callables so `retry_function` can invoke them without creating a circular dependency with `call_api`.

- `fetch_statistics_transform`
- `fetch_matches_elo`
- `fetch_lifetime_url`
- `fetch_match`

---

### `retry_function(function, *args, **kwargs)`

Wraps any fetch function and handles transient failures automatically so the rest of the pipeline doesn't need to.

**Failure behaviour:**

| Error | Behaviour |
|---|---|
| HTTP 429 | Respects `Retry-After` header if present; otherwise exponential backoff |
| HTTP 504 | Fixed 10-second wait, then retry |
| `EmptyData` | Returns empty list immediately, no retry |
| Max retries (5) exceeded | Raises `SkippingMatch` — pipeline moves to next player |

Returns `(response, data, status)`.

---

### `match(player_id, session, randomized=False)`

Fetches a player's CS2 match history from the last 90 days. Fires three paginated requests concurrently (offsets 0, 100, 200) for up to 300 matches total.

This is the first step for both stages of the pipeline — almost every downstream function depends on the match IDs this produces.

When `randomized=False`, returns the raw match list. When `randomized=True` (used by `alter_function`), passes the IDs to `match_randomizer` and returns alter-format output instead.

---

### `match_randomizer(match_ids, num_matches)`

Takes a player's full match list and returns a random sample, filtering out any matches already stored in MongoDB first.

The deduplication step prevents concurrent runs from producing duplicate documents. NumPy's `default_rng` handles the sampling to avoid order-dependent bias.

Raises `SkippingMatch` if the list is empty or all matches already exist in the database.

---

### `alter_function(player_id, session)`

The graph expansion step. Fetches a player's matches, samples them, and extracts every co-player (*alter*) who appeared in those matches. Those alters become the Stage 2 candidates.

If fewer than 15 matches are found the player is skipped. Otherwise raw match documents are added to `matches_batch` and alter records (match ID → player ID list) are added to `alters_batch`.

Returns `(alter_match_ids, alter_data, alters)`.

> `match()` handles the raw fetch and pagination. `alter_function()` owns the sampling decision, the skip logic, and the batch writes. They are separate to keep API logic and pipeline logic independent.

---

### `statistics_transform(match_id, session, count)`

Fetches full match statistics and produces the document written to the `ratings` collection.

Iterates `rounds → teams → players`, splits players into two factions, strips non-numeric fields (team name strings, nicknames), then calls `convert_json` on each faction to compute team-level aggregates. The final document merges those aggregates with the raw `team_stats` from the API.

Returns:
```json
{
  "_id": "<match_id>",
  "players": [...],
  "team_agg": {
    "faction1": { "<stat>": "<mean>", "<team_stat>": "..." },
    "faction2": { "<stat>": "<mean>", "<team_stat>": "..." }
  },
  "stageId": "stage_1_YYYYMMDD_HHMM"
}
```

---

### `convert_json(incoming_json)`

Converts a list of player stat dicts into a single aggregated dict by computing column-wise means. Used internally by `statistics_transform` to produce faction-level features.

Loads the list into a Polars DataFrame, casts to `Float32`, calls `.mean()`, and returns a named dict. Raises `SkippingMatch` if the input is empty.

---

### `matches_elo(match_id, session, count)`

Fetches match-level metadata and strips it down to only what's needed for the `matches_elo` collection: timestamps, results, map pick, faction ELO ratings, and per-player skill levels. Everything else is deleted from the response in place before the document is returned.

---

### `lifetime_aggregates(player_id, session)`

Fetches per-map historical stats for a player from the Faceit lifetime endpoint. These are the stable long-run features used by the ML pipeline — map-specific win rates and average performance metrics.

Strips image keys from segment data and renames `player_id` → `_id` before returning.

---

### `collect_N()`

Returns the player IDs to process in the current run. Behaviour is stage-aware:

- **Stage 1** — queries `players` collection and returns all stored IDs
- **Stage 2** — diffs Stage 1 alter IDs against Stage 2 lifetime IDs, returning only players not yet processed

This is what makes both stages resumable after an interruption — the diff means already-processed players are never re-fetched.

Returns `(indices_array, player_ids)`.

---

### `retrieve_hub_members(hub_id)`

Setup function. Fetches all members of a Faceit hub (paginated, 50 per page), keeping only `user_id` (renamed to `_id`), `nickname`, and `faceit_url`. Saves a CSV checkpoint to `checkpoint_members.csv` when pagination ends.

Run once to populate the `players` collection before the first pipeline run.

---

### `retrieve_ID_members(nicknames, game='cs2', status=True)`

Resolves a list of Faceit nicknames to their player IDs. Used during seeding when the starting player list is a set of known usernames rather than hub members.

---

### `detect_soft_rate_limit(data, skip=False)`

Detects when the Faceit API is silently throttling by tracking empty or skipped responses. The API sometimes returns empty data rather than a proper 429, so this catches that pattern and raises `SoftRateLimit` if it occurs three times in a row.

---

## `orch.py` — `getdata`

Manages the MongoDB connection and provides the write interface used by `PipelineRunner`.

---

### `connect_db(host, port, connect=True)`

Connects to MongoDB and initialises all collection handles and in-memory batch lists. Call once on startup.

| Collections | Batch lists |
|---|---|
| `matches`, `players`, `ratings` | `matches_batch`, `players_batch`, `ratings_batch` |
| `alters`, `lifetime`, `matches_elo` | `alters_batch`, `lifetime_batch`, `matches_elo_batch` |

---

### `store_data(batch, collection, verbose=False)`

The only write path in the pipeline. All storage goes through here.

Write behaviour varies by collection:

| Collection | Method | Notes |
|---|---|---|
| `matches`, `players`, `ratings`, `alters`, `matches_elo` | `insert_many` (unordered) | Skips duplicates silently |
| `lifetime` | `update_one` with upsert | Merges into existing document by `_id` |

Duplicate key errors and bulk write errors are caught and logged without raising — the pipeline continues.

Setting `verbose=True` logs a DataFrame preview of the incoming batch before writing. Useful for debugging but slow on large batches.

---

### `getcol(db, col)`

Returns a MongoDB collection handle for the given database and collection name. Useful for one-off queries outside the standard collections.

---

## `runner.py` — `PipelineRunner`

Orchestrates the full pipeline. Owns concurrency, the async queue, batch flushing, and checkpointing.

---

### `supermatch()`

The main entry point. Everything starts here.

Opens an `aiohttp` session, calls `most_recent_checkpoint()` to determine which players still need processing, prompts for confirmation, then dispatches to `helper_supermatch`. Calls `checkpointer()` before exiting on any failure or keyboard interrupt.

---

### `helper_supermatch(...)`

The concurrent processing loop. Processes up to 6 players simultaneously via semaphore. For each player:

1. Calls `alter_function` to get sampled match IDs
2. Fires `statistics_transform`, `matches_elo`, and `lifetime_aggregates` concurrently via `asyncio.gather`
3. Assembles the result dict and puts it on the queue

**Concurrency limits:**

| Semaphore | Limit | Scope |
|---|---|---|
| Outer | 6 | Concurrent players |
| Inner | 3 | Concurrent API calls per player |
| `lifetime_semaphore` | 2 | Global concurrent lifetime calls |

After all players are dispatched, `queue.join()` blocks until the consumer has fully drained the queue.

---

### `batch_processor()`

The consumer. Runs as a background task, pulling results from the queue and accumulating them in `self.batches`. When the batch reaches `batch_size` (default 30), it flushes all pending results to MongoDB via `store_data` and resets.

Runs indefinitely and is cancelled cleanly after `queue.join()` resolves.

---

### `checkpointer()`

Saves pipeline state to disk on any failure or interrupt. Writes two files:

`checkpoints/checkpoint_<date>.json`:
```json
{
  "time": "<UAE timezone timestamp>",
  "last_player_id_upstream": "<last player sent to queue>",
  "last_player_id_downstream": "<last player written to DB>",
  "batch_Id": 12
}
```

`response/response_<date>.json` — full audit log with per-call latency, status, RPM, average latency, and total execution time.

---

### `most_recent_checkpoint()`

Finds which players still need to be processed so the pipeline can resume after an interruption.

- **Stage 2** — delegates to `collect_N()` which performs the alter/lifetime diff
- **Stage 1** — runs a `$lookup` aggregation joining `players` against `lifetime` on `_id`, returning only players with no matching lifetime document

Returns `(indices_array, player_ids)`.

---

### `backup_func(primary, foreign, key_to_merge='stageId')`

Backfills missing `stageId` values across collections by joining from another collection. Useful if a collection was written before `stageId` tagging was in place.

Runs a `$lookup → $set → $unset → $merge` aggregation pipeline directly on the collection.

```python
# Example: backfill stageId on matches_elo using ratings as the source
runner.backup_func(
    primary=('matches_elo', '_id'),
    foreign=('ratings', '_id')
)
```
