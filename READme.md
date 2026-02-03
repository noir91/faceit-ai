# Faceit AI predictor Chrome extension

### What this is
Chrome extension that collects current team data at inference time and uses ML to do display conditional probabilities of a map from map pool.

### Current state
- Data collection pipeline almost complete, script left.
- ~48 hours of manual iteration
- Model training: not started / baseline only

### Why this exists
Learning and developing prediction for Faceit competitive players to reach higher elo using ML.

### Design choices
The Design choices will be constantly updated as the project continues to evolve.

**Performance Metric:** Brier Score
Predictions using Conditional probabilities of a map outcome given the **map** and **context**(features)

Currently, the **data collection** strategy is to used ego-centric sampling.

Where,
* **N** belongs to handpicked players to extrapolate the network from.
* **M** Matches are samples, not individual players.
* Players fetched from matches are called **alters** 

**Note:** *Not a personalized recommender system or a Learning-To-Rank system.*

We have **n** number of players from set **N**, around **2076** currently. In the following steps we collect matches by:
- Iterate through set **N**
- Collect **M** 15 matches at **random** from the past 40days *(current_time - 40days_ago)* for each **n**
- For each match **m** from **M** get players **alters** involved, and store them with the respective match played.
- Use players **alters** to now, find **X** number of matches to create **training data**.

Currently, all data is fetched from Faceit Data API and then goes to a locally hosted **MongoDB** on ext4fs.

**Source of Data:** FACEIT Data API


