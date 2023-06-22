# mhws-esm

## Installing `marineHeatWaves`
`marineHeatWaves` [by Eric Oliver](https://github.com/ecjoliver/marineHeatWaves), which implements the [Hobday et al. 2016](https://doi.org/10.1016/j.pocean.2015.12.014) definition of marine heatwaves, is a prerequisite for `mhws-esm`. Install this repository directly from Github using the following `pip` command:  
`pip3 install git+https://github.com/ecjoliver/marineHeatWaves.git`  
Note that the install instructions at the marineHeatWaves repository are obsolete; Python no longer allows packages to be installed through the `setup.py` command.

## FAQ
### How do I choose the correct number of processes for parallel processing?
The theoretical maximum quantity of processes (set by the `numProc` variable in various scripts) for best efficiency based on CPU resources is one fewer than the number of threads available in the system. This value can be found by using the command `multiprocessing.cpu_count()-1`. **However, the CPU constraint ignores memory constraints--and the output files will be garbled and unusable if the system runs out of memory during processing.** Thus, the user must also ensure that opening `numProc` number of files in parallel will never exceed the maximum amount of memory (physical + virtual) available on the machine. Note that on shared high-performance computing environments, the memory constraint can be MUCH stricter than the CPU constraint. (On the CSU cluster, which does not allow overflow into virtual memory, this reduced the number of files I could process simultaneously by an order of magnitude.)

## Reminders
- [x] Add all useful outputs (frequency, category, mean intensity, duration) - 4/15
- [ ] Make requirements text file (don't forget to include marineHeatWaves and icecream)
- [ ] Fill out readme with workflow
- [ ] Make in-code documentation and style PEP8 consistent (as relevant)
- [x] Document whole workflow in code (regrid, mergefile, shatter, calculate)
- [x] Determine number of processes
- [x] Link to marineHeatWaves and describe install
- [x] Optimize and update regridding code
