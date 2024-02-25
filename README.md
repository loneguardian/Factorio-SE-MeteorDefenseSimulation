# Factorio: Space Exploration meteor defense simulation

This is a Python SimPy code to simulate meteor defense in Factorio: Space Exploration mod (0.6.125).

## Instruction

1. Review `mdi_sim.py` for Factorio: SE parameters.
1. Use the following console commands to output zone parameters into the factorio log:

    ```
    /c log(serpent.block(remote.call("space-exploration", "get_zone_from_name", {zone_name = "Nauvis"})))
    /c log(serpent.block(remote.call("space-exploration", "get_zone_from_name", {zone_name = "Calidus"})))
    ```

    The following values were used for this repo:
    ```
    Nauvis
    star_gravity_well_zone = 12.066600000000003

    Calidus
    star_gravity_well_star = 18.564
    ```

1. Review `run.py` for simulation parameters.
1. Run `main()` in `run.py` to run the simulation.
1. Simulation output is saved as parquet files into the `output` folder (one file per task chunk).
1. Use codes in `analyse.py` to analyse simulation output.

## To-do

* Implement separate process for serialisation, logging and progress reporting.
