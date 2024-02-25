# Factorio: Space Exploration meteor defense simulation

This is a Python SimPy code to simulate meteor defense in Factorio: Space Exploration mod (0.6.125).

## Instruction

1. Review `mdi_sim.py` for Factorio: SE parameters.
2. Review `run.py` for simulation parameters.
3. Run `main()` in `run.py` to run the simulation. Simulation output is saved as `output.parquet`.
4. Use codes in `analyse.py` to analyse simulation output.

## To-do

* Implement separate process for serialisation, logging and progress reporting.
* Incorporate star gravity into the formula for next meteor strike:

    ```
    modifier_gravity = min(1, (Star gravity well zone / Star gravity well star) * 1.5)

    upper_limit_modifier = 1 + (1 - modifier_gravity) * 3

    actual_meteor_interval = 1 + upper_limit_modifier * random.random() * meteor_interval_upper_limit
    ```

    ```
    Nauvis
    star_gravity_well_zone = 12.066600000000003
    star_gravity_well_star = 18.564
    ```

    ```
    /c log(serpent.block(remote.call("space-exploration", "get_zone_from_name", {zone_name = "Nauvis"})))
    /c log(serpent.block(remote.call("space-exploration", "get_zone_from_name", {zone_name = "Calidus"})))
    ```
