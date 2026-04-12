## Faceit AI Predictor Chrome Extension

#### Version
Current Version: **v0.1.4**

---

### Overview

A Chrome extension that collects **live match data during map veto** and uses a machine learning model to display **conditional win probabilities** for maps in the current map pool.

The goal is to help **Faceit competitive players make better veto decisions**.

---

### System Pipeline
```
Faceit API
    ↓
Data Collection Pipeline
    ↓
MongoDB
    ↓
Feature Engineering
    ↓
Model Training
    ↓
Chrome Extension Inference

```
---

### Current State

- Data collection pipeline **complete**
- Pipeline runner integeration **complete**

- Remaining work:
  - Model training and evaluation
  - Extension inference integration

---

### Why This Exists

This project exists for both:

- **Machine Learning experimentation**
- Building a **decision-support tool** for competitive Faceit players trying to improve their ELO.

---

### Prediction Design

#### Performance Metric
**Brier Score**

Used to evaluate the quality of probabilistic predictions.

#### Model Objective

Predict:
> **P(win | map, context)**

Where **context** represents match features such as team statistics and historical performance.
The model outputs **conditional probabilities of winning each map** in the veto pool.

---

### Data Collection Strategy

The project uses an **ego-centric sampling strategy**.

Where:

- **N** = Handpicked seed players  
- **M** = Matches sampled from those players  
- **Alters** = Other players discovered from sampled matches  

#### Current Dataset
|N| ≈ 2076 players

#### Collection Process

1. Iterate through players in set **N**
2. Sample **M = 15 matches randomly** from the last **40 days**
current_time - 40_days

3. For each match **m**:
   - collect all participating players (**alters**)
4. Use discovered **alters** to expand the dataset by fetching additional matches.
5. Current approach is to ideally use **5-10** matches per alter.

This gradually builds a **network of players and matches** suitable for training.

---

### Data Storage

All data is fetched from the **Faceit Data API** and stored in a locally hosted:

**MongoDB**

Filesystem:
ext4

---

### Development Progress

#### 7 March 2026
Recent updates:

- Refactored functions into **separate modules**
- Introduced **PipelineRunner class**
- Added:
  - **batch processing**
  - **chunking**
- Implemented **retry logic with exponential backoff**
- Added **3 second timeout** for all API requests

---

### Notes

This project **is NOT**:

- A personalized recommender system
- A learning-to-rank model

The system predicts **map win probabilities**, not player rankings.


#### 20 March 2026
Recent updates:

- Fixed Retry function making retry attempts on data errors, not network errors:
    e.g. several player ids were failing to get data, and it was highly predictable which player id will fail to retrieve response from API, poor excecption logic routed an empty response [] data validation error as a networking error and created useless attempts at a "Match not found for **X** player".
- Introudced a checkpointing system to save progress on failure of the runner script at anypoint.
- Implementing detailed logs throughout the system for observability. 
- Introduced a system to record latency per API Call
    - RPM (Requests per second)
    - Average Latency
    - Total Execution Time for Runner Script
    - Total Retry attempts across API calls
- Removed unstable GET requests by implementing sessions.
- /stats and /matches end points now have persistence to avoid redundant API calls.
- Introduced SoftRateLimit logic, Faceit Data API GET 200 to avoid crash at [] empty responses.
- Robust Error handling, fixed all previous runner script exceptions which caused crashes.
- Introduced New functions to capture a richer dataset:
    - **matches_elo** extracts average elo per lobby, skill level per player, and won map with detailed results.
    - **lifetime_aggregates** extracts lifetime aggregate statistics of each player, on each of their maps ever played.
- Fixed checkpoint system failing due to index out of range on last saved checkpoint player id
- /pipeline/READme.md has been updated with latest workflow and details.

**FACEIT Data API**
