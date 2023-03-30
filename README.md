# mhws-esm

## Installing `marineHeatWaves`
`marineHeatWaves` [by Eric Oliver](https://github.com/ecjoliver/marineHeatWaves), which implements the [Hobday et al. 2016](https://doi.org/10.1016/j.pocean.2015.12.014) definition of marine heatwaves, is a prerequisite for `mhws-esm`. Install this repository directly from Github using the following `pip` command:  
`pip3 install git+https://github.com/ecjoliver/marineHeatWaves.git`  
Note that the install instructions at the marineHeatWaves repository are obsolete; Python no longer allows packages to be installed through the `setup.py` command.

## Reminders
- [ ] Make requirements text file (don't forget to include marineHeatWaves and icecream)
- [ ] Fill out readme
- [x] Document whole workflow in code (regrid, mergefile, shatter, calculate)
- [x] Determine number of processes
- [x] Link to marineHeatWaves and describe install
- [x] Optimize and update regridding code
