## Faceit AI Predictor Chrome Extension

#### Version
Current Version: **v0.1.3**

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

- Data collection pipeline **almost complete**
- Remaining work:
  - Pipeline runner integration
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

---

### Data Source
**FACEIT Data API**
