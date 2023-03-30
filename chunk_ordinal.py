''' chunk_embed_ordinal 
Embed ordinal time using Dask chunking '''

from icecream import ic
import sys

import glob
import numpy as np
import xarray as xr

import necessary_helper_fun as nhf

dataDict = { # Provide input path 
    "dataPath": '/Users/dhueholt/Documents/mhws_data/daily_SST/full/',
    "idGlensCntrl": None, #'*control*' or None
    "idGlensFdbck": None, #'*feedback*' or None
    "idArise": None, #'*SSP245-TSMLT-GAUSS*' or None
    "idS245Cntrl": '*BWSSP245*', # '*BWSSP245*' or None
    "idS245Hist": None #'*BWHIST*' or None
}
outDict = {
    "saveFlag": True,
    "savePath": '/Users/dhueholt/Documents/mhws_data/ordinal/',
    "outPrefix": 'mhwDefsFile_'
}

# Open raw data
inPath = dataDict["dataPath"] + dataDict["idS245Cntrl"]
inGlobs = sorted(glob.glob(inPath)) # Needed to make filenames for saving
inDset = xr.open_mfdataset(
    inPath, concat_dim='realization', chunks={'lat': 64}, combine='nested', 
    coords='minimal')

# Resample to make sure all timesteps have an entry
dsetFullTimes = inDset.resample(time='1D').asfreq()

# Convert to ordinal time, as needed by marineHeatWaves package
resampleTimes = dsetFullTimes.time.data
ordArr = nhf.make_ord_array(resampleTimes)

# Save an individual file for each member
for r in inDset.realization.data:
    rfd = nhf.mine_file_str(inGlobs[r])
    climDset = xr.Dataset(
        {rfd["var"]: (("time", "lat", "lon"), 
            dsetFullTimes[rfd["var"]].isel(realization=r).data.compute())},
        coords={
            "time": (('time'), ordArr),
            "lat": dsetFullTimes.lat,
            "lon": dsetFullTimes.lon
        }
    )
    climDset[rfd["var"]].attrs = dsetFullTimes.attrs
    climDset[rfd["var"]].attrs['long_name'] = 'SST' #Custom long_name if needed
    # ic(climDset) # Useful for troubleshooting
    outFile = outDict["savePath"] + outDict["outPrefix"] + rfd["scn"] \
        + '_' + rfd["rlz"] + '_' + rfd["var"] + '_' + rfd["times"] \
        + '.nc'
    ic(outFile)
    if outDict["saveFlag"]:
        climDset.to_netcdf(outFile)

# Calculate ensemble mean and save separate file
rlzMnDset = dsetFullTimes.mean(dim='realization')
rlzMnClimDset = xr.Dataset(
    {rfd["var"]: (("time", "lat", "lon"),
        rlzMnDset[rfd["var"]].data.compute())},
    coords={
        "time": (('time'), ordArr),
        "lat": dsetFullTimes.lat,
        "lon": dsetFullTimes.lon
    }
)
rlzMnClimDset[rfd["var"]].attrs = dsetFullTimes.attrs
rlzMnClimDset[rfd["var"]].attrs['long_name'] = 'SST' #Custom long_name if needed
rlzMnOutFile = outDict["savePath"] + outDict["outPrefix"] + rfd["scn"] \
    + '_' + 'rlzMn' + '_' + rfd["var"] + '_' + rfd["times"] + '.nc'
ic(rlzMnOutFile)
if outDict["saveFlag"]:
    climDset.to_netcdf(rlzMnOutFile)