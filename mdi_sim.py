import math
import random

import polars as pl
import simpy as sim

# Parameters

METEOR_INTERVAL_SETTING = 30
STAR_GRAVITY_WELL_ZONE = 12.066600000000003
STAR_GRAVITY_WELL_STAR = 18.564
DEEP_SPACE = False
MDI_COUNT = 24
EVENT_HORIZON = 60000

# Constants

METEOR_COUNT_MAX = 100
MDI_FIRING_INTERVAL = 30 / 3600
MDI_HIT_CHANCE = 0.8
MDI_RECHARGE_TIME = 20 / 9

# Meteor interval modifiers

STAR_GRAVITY_MODIFIER = (
  (DEEP_SPACE is False)
  and min(1, (STAR_GRAVITY_WELL_ZONE / STAR_GRAVITY_WELL_STAR) * 1.5)
  or 0
)
METEOR_INTERVAL_MODIFIER = 1 + (1 - STAR_GRAVITY_MODIFIER) * 3

df_schema = {
  "id": pl.UInt16,
  "time": pl.Float32,
  "meteor": pl.UInt16,
  "mdi_ready": pl.UInt16,
  "shot_fired": pl.UInt16,
  "meteor_shot": pl.UInt16,
}


class Mdi:
  def __init__(self, id: int, env: sim.Environment, ready_store: sim.Store) -> None:
    self.id = id
    self.env = env
    ready_store.put(self)
    self.ready_store = ready_store
    self.mdi_proc = env.process(self.mdi_loop())

  def fire(self):
    self.is_fired.succeed()

  def mdi_loop(self):
    while True:
      self.is_fired = self.env.event()
      yield self.is_fired
      yield self.env.timeout(MDI_RECHARGE_TIME)
      yield self.ready_store.put(self)


def mdi_proc(
  env: sim.Environment,
  meteor_store: sim.Store,
  mdi_ready_store: sim.Store,
  data: list,
  cycle_id: int,
):
  while True:
    meteor_count = yield meteor_store.get()

    shot_fired = 0
    meteor_shot = 0
    mdi_ready_count = len(mdi_ready_store.items)

    while True:
      mdi: Mdi = yield mdi_ready_store.get()
      mdi.fire()
      shot_fired += 1
      if random.random() < MDI_HIT_CHANCE:
        meteor_shot += 1
      if (meteor_shot == meteor_count) or (len(mdi_ready_store.items) == 0):
        break
      else:
        yield env.timeout(MDI_FIRING_INTERVAL)

    data.append(
      (
        cycle_id,
        env.now,
        meteor_count,
        mdi_ready_count,
        shot_fired,
        meteor_shot,
      )
    )


def meteor_proc(env: sim.Environment, meteor_store: sim.Store):
  while True:
    yield env.timeout(1 + METEOR_INTERVAL_MODIFIER * random.random() * METEOR_INTERVAL_SETTING)
    meteor_count = math.floor(math.log(1 / (1 - random.random()), 2)) + 1
    yield meteor_store.put(meteor_count)


def sim_cycle(cycle_id: int = 0):
  data = []
  env = sim.Environment()

  # Stores
  meteor_store = sim.Store(env)
  mdi_ready_store = sim.Store(env)

  # Instantiate MDIs
  for i in range(MDI_COUNT):
    Mdi(i + 1, env, mdi_ready_store)

  env.process(meteor_proc(env, meteor_store))
  env.process(mdi_proc(env, meteor_store, mdi_ready_store, data, cycle_id))

  env.run(until=EVENT_HORIZON)

  return data


def sim_chunk(input: tuple):
  worker_id = input[0]
  cycles = input[1]
  data = []

  for cycle in cycles:
    data.extend(sim_cycle(cycle))

  lf = pl.LazyFrame(data, df_schema, orient="row")
  lf.sink_parquet(f"output/output-{worker_id}.parquet")
