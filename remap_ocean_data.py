'''  remap_ocean_data
Runs functions to remap ocean model output from the ocean grid into standard
lat/lon coordinates and save as a new netCDF file. Currently, this code works
for output from the Parallel Ocean Program (POP) and the Nucleus for European
Modelling of the Ocean (NEMO) models.

Input variables are located at the start of the script; those too 
complicated to be explained by in-line comments are described here.
dataVar: Manually provide variable name for the input files, examples below.
    'TEMP': ocean temperature (CESM)
    'tos': sea surface temperature (CMIP6-formatted)
ocnDimNames: Provide dimension names for the starting grid.
    For POP (CESM): 'TLAT', 'TLONG'
    For NEMO (UKESM): 'latitude', 'longitude'
regridFun: Function from fun_remap to use for regridding.
    For POP (CESM): fr.operate_regrid_cesm
    For NEMO (UKESM): fr.operate_regrid_ukesm
        
NOTE: The multiprocessing documentation claims 'fork' functionality only works 
on Unix-based systems (e.g., Mac OS, Linux). Thus, this code likely will not
run on Windows, although I have not verified this.

Written by Daniel Hueholt
Graduate Research Assistant at Colorado State University
Based on code originally written by Emily Gordon and Zachary Labe.
'''
import glob
import multiprocessing as mp
import numpy as np
import sys
import xarray as xr

from icecream import ic
import fun_remap as fr

# Inputs - see docstring for details
dataPath = '/Users/dhueholt/Documents/ecology_data/monthly_OCNTEMP500/shortForRgTest/'
outPath = '/Users/dhueholt/Documents/mhwwg_out/' # Location to save regridded files
dataVar = 'TEMP'
nProc = 2 # Spawn nProc+1 number of processes for regridding
ocnDimNames = ['TLAT', 'TLONG']
regridFun = fr.operate_regrid_cesm

# Open files and extract ocean coordinates
strList = sorted(glob.glob(dataPath + "*.nc"))
dataList = list()
for fc,fv in enumerate(strList):
    inDset = xr.open_dataset(fv) # Open files separately as times may not match
    inDarr = inDset[dataVar]
    inDarr.attrs['inFile'] = fv.replace(dataPath,"") # Track individual files
    inDarr.attrs['outPath'] = outPath
    dataList.append(inDarr) # Place all in single list

ocnCd1, ocnCd2 = fr.extract_ocn_latlons(
    inDarr, ocnDimNames[0], ocnDimNames[1])

# Regrid to standard lat/lon and save
for dc, dv in enumerate(dataList):
    lenDat = len(dataList)
    if __name__== '__main__': # If statement required by multiprocessing
        mp.set_start_method('fork', force=True)
        proc = mp.Process( # Each parallel process regrids and saves a file
            target=regridFun, args=(dv, ocnCd1, ocnCd2))
        if dc % nProc == 0 and dc != 0: # Every nProc number of files
            proc.start()
            proc.join() # Complete nProc+1 processes before starting more
            proc.close() # Free up resources to lower load on machine
        else:
            proc.start()
        filesRemaining = lenDat - dc - 1
        numleft = ic(filesRemaining) # Cheap "progress bar"
