The python scripts stored here are sourced from <https://github.com/Sage-Bionetworks/schematic_db>.
This logic was extracted out of `schematic_db` as there were a new of required 
dependency updates that prevented using the updated `schematic_db` code. Those
dependency updates included:

- Great expectations
- Pydantic
- tenacity
- Discontinuing python 3.9

As such the following considerations were made:

- Extract the required functionality out of `schematic_db` such that `schematic` can
continue to function with the current dependencies, but, updates to the dependent code
may still occur.
- Functionality that exists within this extracted code should be split between
application (schematic) specific business logic, and core (SYNPY) logic. This will start
to come to fruition with SYNPY-1418 where table functionality is going to be expanded.
