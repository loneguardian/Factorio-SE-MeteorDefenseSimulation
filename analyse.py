# %%
import polars as pl
import polars.selectors as cs
from pathlib import Path

lf_list = []

for path in Path("output").glob("*.parquet"):
  lf_list.append(pl.scan_parquet(path))

lf: pl.LazyFrame = pl.concat(lf_list).with_columns(
  (pl.col("meteor") - pl.col("meteor_shot")).alias("remaining")
)

lf.drop("time", "id").describe()

# %%
lf_2 = (
  lf.drop("time")
  .group_by("id")
  .agg(cs.exclude("mdi_ready").sum().name.suffix("_sum"))
  .drop("id")
)

lf_2.describe()

# %%
lf_2.group_by("remaining_sum").len().sort("remaining_sum").collect()

# %%
