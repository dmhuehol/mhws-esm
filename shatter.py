''' shatter
Breaks up ("shatters") a dataset into more manageable pieces ("shards") 
for future processing. Additionally, resamples data to a common time 
axis and embeds ordinal times. These data characteristics are necessary 
to calculate marine heatwaves using marineHeatWaves in "calc_shard_mhws".

#### INPUTS ####
dataDict: defines input data
    dataPath: input path of data files to be shattered
    dataToken: suggested tokens for datasets below
        ARISE-SAI-1.5: '*SSP245-TSMLT-GAUSS-DEFAULT*'
        SSP2-4.5: '*BWSSP245*'
outDict: specify features of output data
    shardSize: number of degrees lat in each shard file
        32 produces data sizes of ~1GB for SSP2-4.5 (2015-2065) and
        ~270MB SSP2-4.5 (2015-2024). This strikes a good balance as
        shard sizes between 100MB and 2GB are generally optimally
        efficient. The best shardSize may vary for other datasets.
    saveFlag: True/False to save/not save output files
    calcEachRlz: True/False to save/not save shards for each realization
    calcRlzMn: True/False to save/not save shards for realization mean
    savePath: path to save shard files
    outPrefix: optional prefix to append to output file names

Written by Daniel Hueholt
Graduate Research Assistant at Colorado State University 
'''
import glob
import sys

import numpy as np
from icecream import ic
import xarray as xr

import fun_mhws as fm

##### INPUTS: SEE DOCSTRING FOR DOCUMENTATION ####
dataDict = { 
    # "dataPath": '/Users/dhueholt/Documents/mhws_data/daily_SST/full/',
    "dataPath": '/glade/scratch/dhueholt/daily_SST/full/',
    "dataToken": '*SSP245-TSMLT-GAUSS-DEFAULT*'
}
outDict = {
    "shardSize": 32,
    "saveFlag": True,
    "calcEachRlz": False, 
    "calcRlzMn": True,
    "savePath": '/glade/scratch/dhueholt/daily_SST/shards/data/ARISE-SAI-1.5/',
    "outPrefix": ''
}

# Open raw data
ic('Opening data!')
inPath = dataDict["dataPath"] + dataDict["dataToken"]
inGlobs = sorted(glob.glob(inPath)) # Needed to make filenames for saving
inDset = xr.open_mfdataset(
    inPath, concat_dim='realization', chunks={'lat': 64}, combine='nested', 
    coords='minimal')

# Resample to ensure all timesteps have an entry
ic('Resampling times!')
dsetFullTimes = inDset.resample(time='1D').asfreq()

# Convert to ordinal time, as needed by marineHeatWaves package
resampleTimes = dsetFullTimes.time.data
ordArr = fm.make_ord_array(resampleTimes)

# Set indices to shatter array into "shards"
frct = np.arange(
    0, len(inDset.lat.data)+1, outDict["shardSize"]) # "fractures"

# Save individual shards for each member
if outDict["calcEachRlz"]:
    ic('Processing individual members!')
    for r in inDset.realization.data:
        rfd = fm.mine_file_str(inGlobs[r])       
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
    rfd = fm.mine_file_str(inGlobs[0]) # Need this even if individual members aren't saved
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