''' shatter
Embed ordinal time and save files in shards for easier processing
and parallelization '''

from icecream import ic
import sys

import glob
import numpy as np
import xarray as xr

import necessary_helper_fun as nhf

##### INPUTS ####
dataDict = { 
    "dataPath": '/Users/dhueholt/Documents/mhws_data/daily_SST/defPeriod/', # Input path 
    "idGlensCntrl": None, #'*control*' or None
    "idGlensFdbck": None, #'*feedback*' or None
    "idArise": None, #'*SSP245-TSMLT-GAUSS*' or None
    "idS245Cntrl": '*BWSSP245*', # '*BWSSP245*' or None
    "idS245Hist": None #'*BWHIST*' or None
}
outDict = {
    "shardSize": 32, # Number of degrees lat in each shard file
    "saveFlag": True, # True/False to save/not save output files
    "calcEachRlz": False, # Save shards for each realization
    "calcRlzMn": True, # Save shards for realization mean
    "savePath": '/Users/dhueholt/Documents/mhws_data/ordinal/', # Output path
    "outPrefix": '' # Prefix to append to output file names
}

# Open raw data
ic('Opening data!')
inPath = dataDict["dataPath"] + dataDict["idS245Cntrl"]
inGlobs = sorted(glob.glob(inPath)) # Needed to make filenames for saving
inDset = xr.open_mfdataset(
    inPath, concat_dim='realization', chunks={'lat': 64}, combine='nested', 
    coords='minimal')

# Resample to make sure all timesteps have an entry
ic('Resampling times!')
dsetFullTimes = inDset.resample(time='1D').asfreq()

# Convert to ordinal time, as needed by marineHeatWaves package
resampleTimes = dsetFullTimes.time.data
ordArr = nhf.make_ord_array(resampleTimes)

# Set indices to shatter array into "shards"
frct = np.arange(
    0, len(inDset.lat.data)+1, outDict["shardSize"]) # "fractures"

# Save individual shards for each member
if outDict["calcEachRlz"]:
    ic('Processing individual members!')
    for r in inDset.realization.data:
        rfd = nhf.mine_file_str(inGlobs[r])       
        for fc,fv in enumerate(frct[:-1]): # Break up array at predefined indices
            actRlz = dsetFullTimes[rfd["var"]].isel(realization=r)
            frg = (fv, frct[fc+1]) # Defining bounds of fragment
            shard = actRlz.isel(
                lat=np.arange(frg[0], frg[1])) # Shatter
                
            climDset = xr.Dataset(
                {rfd["var"]: (("time", "lat", "lon"), 
                    shard.data.compute())},
                coords={
                    "time": (('time'), ordArr),
                    "lat": shard.lat,
                    "lon": shard.lon
                }
            )
            climDset[rfd["var"]].attrs = dsetFullTimes.attrs
            climDset[rfd["var"]].attrs['long_name'] = 'SST' #Custom long_name if needed
            # ic(climDset) # Useful for troubleshooting
            outFile = outDict["savePath"] + outDict["outPrefix"] + rfd["scn"] \
                + '_' + rfd["rlz"] + '_' + rfd["var"] + '_' + rfd["times"] \
                + '_' + str(frg[0]) + '-' + str(frg[1]) + '.nc'
            ic(outFile)
            if outDict["saveFlag"]:
                climDset.to_netcdf(outFile)

# Calculate ensemble mean and save separate shards for ensemble mean
if outDict["calcRlzMn"]:
    ic('Calculating ensemble mean!')
    rfd = nhf.mine_file_str(inGlobs[0]) # Need this even if individual members aren't saved
    rlzMnDset = dsetFullTimes.mean(dim='realization').compute()   
    for fc,fv in enumerate(frct[:-1]):
        frg = (fv, frct[fc+1]) # Defining bounds of fragment
        shardMn = rlzMnDset.isel(
            lat=np.arange(frg[0], frg[1]))
            
        rlzMnClimDset = xr.Dataset(
            {rfd["var"]: (("time", "lat", "lon"),
                shardMn[rfd["var"]].data)},
            coords={
                "time": (('time'), ordArr),
                "lat": shardMn.lat,
                "lon": shardMn.lon
            }
        )
        rlzMnClimDset[rfd["var"]].attrs = dsetFullTimes.attrs
        rlzMnClimDset[rfd["var"]].attrs['long_name'] = 'SST' #Custom long_name if needed
        rlzMnOutFile = outDict["savePath"] + outDict["outPrefix"] + rfd["scn"] \
            + '_' + 'rlzMn' + '_' + rfd["var"] + '_' + rfd["times"] \
            + '_' + str(frg[0]) + '-' + str(frg[1]) + '.nc'
        ic(rlzMnOutFile)
        if outDict["saveFlag"]:
            rlzMnClimDset.to_netcdf(rlzMnOutFile)