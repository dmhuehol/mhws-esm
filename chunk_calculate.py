''' chunk_calculate 
Calculate MHWs in chunks using dask (hopefully) 
Must have ordinal times already embedded '''

from icecream import ic
import sys
import time

import marineHeatWaves as mhws
import numpy as np
import xarray as xr

import necessary_helper_fun as nhf

dataDict = { # Provide input path 
    "dataPath": '/Users/dhueholt/Documents/mhws_data/ordinal/',
    "idGlensCntrl": None, #'*control*' or None
    "idGlensFdbck": None, #'*feedback*' or None
    "idArise": None, #'*SSP245-TSMLT-GAUSS*' or None
    "idS245Cntrl": '*BWSSP245*', # '*BWSSP245*' or None
    "idS245Hist": None, #'*BWHIST*' or None
    "idTest": 'mhwDefsFile_BWSSP245cmip6_rlzMn_SST_20150101-20650101_RG.nc'
}
mDefDict = {
    "defPath": '/Users/dhueholt/Documents/mhws_data/ordinal/',
    "idDef": 'mhwDefsFile_BWSSP245cmip6_rlzMn_SST_20150101-20241231_RG.nc'
}
outDict = {
    "outPath": '/Users/dhueholt/Documents/mhwwg_out/'
}

tFile = mDefDict["defPath"] + mDefDict["idDef"]

ticOpenDef = time.time()
mDef = xr.open_dataset(tFile, chunks={'lat': 64})
mDefSst = mDef['SST'].isel(lat=30).isel(lon=3).data.compute()
mDefTimes = mDef.time.data
altClim = list([mDefTimes, mDefSst])
tocOpenDef = time.time() - ticOpenDef
ic(tocOpenDef)

inFile = dataDict["dataPath"] + dataDict["idTest"]
# inPath = dataDict["dataPath"] + dataDict["idS245Cntrl"]
# inGlobs = sorted(glob.glob(inPath)) # Needed to make filenames for saving
# inDset = xr.open_mfdataset(
#     inPath, concat_dim='realization', chunks={'lat': 64}, combine='nested', 
#     coords='minimal')
ticOpenFile = time.time()
rawDset = xr.open_dataset(inFile, chunks={'lat': 64})
sstDat = rawDset['SST'].isel(lat=30).isel(lon=3).data.compute()
tocOpenFile = time.time() - ticOpenFile
ic(tocOpenFile)
sstTimes = rawDset.time.data

ticMhw = time.time()
ic('Start detection')
# ic(np.shape(sstTimes), np.shape(sstDat), np.shape(altClim), np.shape(altClim[0]), np.shape(altClim[1]))
# ic(sstTimes, sstDat, altClim, altClim[0], altClim[1])
mhwsDict, climDict = mhws.detect(
    sstTimes, sstDat, climatologyPeriod=[2015,2024], alternateClimatology=altClim)
tocMhw = time.time() - ticMhw
ic(tocMhw)
# ic(mhwsDict, climDict)
sys.exit('STOP')

binMhwPres = np.zeros(np.shape(sstTimes)) #Initiate blank array of the proper size
mhwStrtInd = mhwsDict['index_start']
mhwDurInd = mhwsDict['duration']
for actInd,startInd in enumerate(mhwStrtInd):
    actDur = mhwDurInd[actInd] #Duration of active MHW
    binMhwPres[startInd:startInd+actDur] = 1 #Times with active MHW get a 1
  
ic(binMhwPres)
ic(np.shape(binMhwPres))
# mhwDset = xr.Dataset(
#     {'binary_mhw': (("time", "lat", "lon"), binMhwPres)},
#     coords = {
#         "time": (('time'), sstTimes),
#         "lat": rawDset.lat,
#         "lon": rawDset.lon
#     }
# )
# mhwDset['binary_mhw'].attrs = rawDset.attrs
# mhwDset['binary_mhw'].attrs['long_name'] = 'Binary presence-absence of MHWs'
# outStr = 'mhws_SSP245_20152065.nc'
# outFile = outPath + outStr
# mhwDset.to_netcdf(outFile)