''' chunk_calculate 
Calculate MHWs in chunks using dask (hopefully) 
Must have ordinal times already embedded 

Tokens: 
    ARISE-SAI-1.5: '*SSP245-TSMLT-GAUSS*'
    SSP2-4.5: '*BWSSP245*'
    '''

from icecream import ic
import sys

import glob
from multiprocessing import Process

import necessary_helper_fun as nhf

dataDict = { 
    "dataPath": '/Users/dhueholt/Documents/mhws_data/shards/data/', # Path of data shards
    "dataToken": '*BWSSP245*', # See docstring for tokens
    "refPath": '/Users/dhueholt/Documents/mhws_data/shards/ref/', # Path of reference shards
    "refToken": '*BWSSP245*' # See docstring for tokens
}
outDict = {
    "outPath": '/Users/dhueholt/Documents/mhwwg_out/', # Path to save MHW files
    "outPrefix": 'testfun_', # Prefix to append to output filenames
    "saveFlag": True # True/False to save/not save output files
}
numProc = 7 # Number of files to process in parallel, set to a maximum of multiprocessing cpus-1 (e.g., multiprocessing.cpu_count()-1) for best efficiency

dataLoc = dataDict["dataPath"] + dataDict["dataToken"]
dataGlobs = sorted(glob.glob(dataLoc))
refLoc = dataDict["refPath"] + dataDict["refToken"]
refGlobs = sorted(glob.glob(refLoc))
ic(dataGlobs, refGlobs)
for rg in refGlobs:
    ic(rg) # Track progress
    refFrg = rg.split('_')[-1]
    for dc,dg in enumerate(dataGlobs):
        if refFrg in dg:
            if __name__ == '__main__':
                proc = Process(target=nhf.calc_marine_heatwaves, args=(dg, rg, outDict))
            if dc % numProc == 0 and dc !=0:
                proc.start()
                proc.join() # Forces numProc+1 processes to run to completion before beginning more
                proc.close() # Free up all associated resources
            else:
                proc.start()

ic('Completed :D')

# tFile = mDefDict["defPath"] + mDefDict["idDef"]

# ticSetupDef = time.time()
# mDef = xr.open_dataset(tFile) # NO chunking, it isn't needed for future operations
# mDefSst = mDef['SST'].data
# mDefTimes = mDef.time.data
# tocSetupDef = time.time() - ticSetupDef; ic(tocSetupDef)

# ticOpenData = time.time()
# inFile = dataDict["dataPath"] + dataDict["idTest"]
# rawDset = xr.open_dataset(inFile) # NO chunking, data laid out efficiently by shatter
# sstDat = rawDset['SST'].data
# sstTimes = rawDset.time.data
# sstLats = rawDset.lat.data
# sstLons = rawDset.lon.data
# tocOpenData = time.time() - ticOpenData; ic(tocOpenData)

# mhwTemplate = np.empty(np.shape(sstDat))
# mhwTemplate.fill(np.nan) # timexlatxlon NaNs to be populated with data

# timerJustCalc = list()
# timerEachMhw = list()
# ticMhwOverall = time.time()
# ic('Start detection')
# for latc, latv in enumerate(sstLats):
#     ic(latc) # Cheap "progress bar"
#     for lonc, lonv in enumerate(sstLons):
#         actDat = sstDat[:, latc, lonc]
#         actDef = mDefSst[:, latc, lonc]
#         ticEachMhw = time.time()
#         altClim = list([mDefTimes, actDef])
#         checkNan = np.isnan(actDat[0]) # NaNs occur over e.g., land
#         if checkNan == False:
#             ticEachMhw = time.time()
#             altClim = list([mDefTimes, actDef])
#             mhwsDict, climDict = mhws.detect(
#                 sstTimes, actDat, climatologyPeriod=[2015,2024],
#                 alternateClimatology=altClim)
#             tocEachMhwJustCalc = time.time() - ticEachMhw #; ic(tocEachMhwJustCalc)
#             timerJustCalc.append(tocEachMhwJustCalc)
#             bnryMhw = np.zeros(np.shape(sstTimes)) # Array for each MHW binary timeseries
#             mhwStrtInd = mhwsDict['index_start']
#             mhwDurInd = mhwsDict['duration']
#             for actInd,startInd in enumerate(mhwStrtInd):
#                 actDur = mhwDurInd[actInd]
#                 bnryMhw[startInd:startInd+actDur] = 1 # Times with active MHW get a 1
#             mhwTemplate[:, latc, lonc] = bnryMhw
#             tocEachMhw = time.time() - ticEachMhw #; ic(tocEachMhw)
#             timerEachMhw.append(tocEachMhw)
        
# tocMhwOverall = time.time() - ticMhwOverall; ic(tocMhwOverall)
# ic(timerJustCalc, np.mean(timerJustCalc))
# ic(timerEachMhw, np.mean(timerEachMhw))
# ic(np.unique(mhwTemplate))

# mhwDset = xr.Dataset(
#     {'binary_mhw': (("time", "lat", "lon"), mhwTemplate)},
#     coords = {
#         "time": (('time'), sstTimes),
#         "lat": rawDset.lat,
#         "lon": rawDset.lon
#     }
# )
# mhwDset['binary_mhw'].attrs = rawDset.attrs
# mhwDset['binary_mhw'].attrs['long_name'] = 'Binary presence-absence of MHWs'
# outStr = inFile.split('/')[-1].replace('SST', 'mhw')
# outFile = outDict["outPath"] + outStr
# ic(outFile)
# mhwDset.to_netcdf(outFile)