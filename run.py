import concurrent.futures as cof
import os
import time
from pathlib import Path

import numpy as np

import mdi_sim

ITERATION_COUNT = 10000
CPU_COUNT = os.cpu_count()


def main():
  with cof.ProcessPoolExecutor(max_workers=CPU_COUNT) as executor:
    task_chunk_arr_list = np.array_split(np.arange(1, ITERATION_COUNT + 1), CPU_COUNT)
    task_chunks = []
    for i in range(len(task_chunk_arr_list)):
      task_chunks.append((i + 1, tuple(task_chunk_arr_list[i].tolist())))
    executor.map(mdi_sim.sim_chunk, task_chunks)


if __name__ == "__main__":
  # clear output folder
  for path in Path("output").glob("**/*.*"):
    if path.is_file():
      path.unlink()

  timer_start = time.perf_counter()
  main()
  print(f"Timer: {time.perf_counter() - timer_start} seconds")
