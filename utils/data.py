from pathlib import Path
import sys, os

parent_dir = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, parent_dir)

import pymongoarrow.api as pmi
from pipeline.orch import getdata
import pyarrow.parquet as pq
import polars as pl
import pyarrow as pa
"""
Takes initial snapshot of documents across multiple ratings and saves
them into their own respective parquets. 

These parquets will be used to assess and transform the data model into
a gold model where it's analytically ready to be put into an ML model.

"""

# paths

BASE = Path(__file__).resolve().parent.parent

ratingpath = BASE / "data/raw/ratings.parquet"
elopath = BASE / "data/raw/matches_elo.parquet"
lifetimepath = BASE / "data/raw/lifetime.parquet"
lfscorespath = BASE / "data/raw/lffscores.parquet"

ratingssd = "/media/noir/SSD/faceit-data-20may/ratings.parquet"
elossd = "/media/noir/SSD/faceit-data-20may/elo.parquet"
lfscoresssd = "/media/noir/SSD/faceit-data-20may/lfscores.parquet"
while True:
    print(os.getcwd())
    p = input("Cancel (Y/N):").lower()

    if p in ['yes', 'y']:
        sys.exit(0)
    else:
        break
dbobj = getdata()
dbobj.connect_db(
    host = 'localhost',
    port = 27017
)
ratings = dbobj.ratings
elo = dbobj.matches_elo
lfscores = dbobj.lfscores
#lifetime = dbobj.lifetime

#ratings_df = pl.from_arrow(pmi.find_arrow_all(ratings, {}))
#elo_df = pl.from_arrow(pmi.find_arrow_all(elo, {}))
#lfscores_df =pl.from_arrow(pmi.find_arrow_all(lfscores, {}))
#lifetime_df = pl.from_arrow(pmi.find_arrow_all(lifetime, {}))

references = {
    #'ratings': [ratings, ratingssd],
    #'elo': [elo, elossd],
    'lfscores': [lfscores, lfscorespath]
    #'lifetime': [lifetime_df, lifetimepath]
}


#for name, (collection, path) in references.items():
#    print(f"{name} writing...")
#    df = pl.from_arrow(pmi.find_arrow_all(collection, {}))
#    df.write_parquet(path)
#    del df  # release RAM before loading next collection
#    print(f"{name} saved.")

cursor = lfscores.find({}, batch_size=5_000)
writer = None
batch = []

for doc in cursor:
    doc['_id'] = str(doc.pop('_id'))
    batch.append(doc)

    if len(batch) >= 5_000:
        table = pa.Table.from_pylist(batch)
        if writer is None:
            writer = pq.ParquetWriter(lfscorespath, table.schema)
        writer.write_table(table)
        print(f"wrote {len(batch):,} rows...")  # ← moved up one line
        batch = []

if batch:
    writer.write_table(pa.Table.from_pylist(batch))

writer.close()
print("done.")


