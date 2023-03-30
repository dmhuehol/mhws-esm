''' fun_mhws
Functions used for calculating MHWs and preprocessing data in the
mhws-esm package.

Written by Daniel Hueholt
Graduate Research Assistant at Colorado State University
'''
import dask
from datetime import date
import marineHeatWaves as mhws
import numpy as np
import sys
import time
import xarray as xr
xr.set_options(keep_attrs=True)

from icecream import ic

def calc_marine_heatwaves(refFile, dataFile, outDict):
    ''' Calculate marine heatwaves from input file relative to a 
    reference climatology '''
    # Open reference file
    # ticSetupDef = time.time()
    mDef = xr.open_dataset(refFile) # NO chunking, it isn't needed for future operations
    mDefSst = mDef['SST'].data
    mDefTimes = mDef.time.data
    # tocSetupDef = time.time() - ticSetupDef; ic(tocSetupDef)
    
    # Open data file
    # ticOpenData = time.time()
    rawDset = xr.open_dataset(dataFile) # NO chunking, data laid out efficiently by shatter
    sstDat = rawDset['SST'].data
    sstTimes = rawDset.time.data
    sstLats = rawDset.lat.data
    sstLons = rawDset.lon.data
    # tocOpenData = time.time() - ticOpenData; ic(tocOpenData)
    
    mhwTemplate = np.empty(np.shape(sstDat))
    mhwTemplate.fill(np.nan) # timexlatxlon NaNs to be populated with data
    
    # Calculate marine heatwaves
    # timerJustCalc = list()
    # timerEachMhw = list()
    # ticMhwOverall = time.time()
    for latc, latv in enumerate(sstLats):
        ic(latc) # Cheap "progress bar"
        for lonc, lonv in enumerate(sstLons):
            actDat = sstDat[:, latc, lonc]
            actDef = mDefSst[:, latc, lonc]
            # ticEachMhw = time.time()
            altClim = list([mDefTimes, actDef])
            checkNan = np.isnan(actDat[0]) # NaNs occur over e.g., land
            if checkNan == False:
                # ticEachMhw = time.time()
                altClim = list([mDefTimes, actDef])
                mhwsDict, climDict = mhws.detect(
                    sstTimes, actDat, climatologyPeriod=[2015,2024],
                    alternateClimatology=altClim)
                # tocEachMhwJustCalc = time.time() - ticEachMhw #; ic(tocEachMhwJustCalc)
                # timerJustCalc.append(tocEachMhwJustCalc)
                bnryMhw = np.zeros(np.shape(sstTimes)) # Array for each MHW binary timeseries
                mhwStrtInd = mhwsDict['index_start']
                mhwDurInd = mhwsDict['duration']
                for actInd,startInd in enumerate(mhwStrtInd):
                    actDur = mhwDurInd[actInd]
                    bnryMhw[startInd:startInd+actDur] = 1 # Times with active MHW get a 1
                mhwTemplate[:, latc, lonc] = bnryMhw
                # tocEachMhw = time.time() - ticEachMhw #; ic(tocEachMhw)
                # timerEachMhw.append(tocEachMhw)           
    # tocMhwOverall = time.time() - ticMhwOverall; ic(tocMhwOverall)
    # ic(timerJustCalc, np.mean(timerJustCalc))
    # ic(timerEachMhw, np.mean(timerEachMhw))
    # ic(np.unique(mhwTemplate))
    
    # Construct an MHWs dataset and save to netCDF
    mhwDset = xr.Dataset(
        {'binary_mhw': (("time", "lat", "lon"), mhwTemplate)},
        coords = {
            "time": (('time'), sstTimes),
            "lat": rawDset.lat,
            "lon": rawDset.lon
        }
    )
    mhwDset['binary_mhw'].attrs = rawDset.attrs
    mhwDset['binary_mhw'].attrs['long_name'] = 'Binary presence-absence of MHWs'
    outStr = dataFile.split('/')[-1].replace('SST', 'mhw')
    outFile = outDict["outPath"] + outDict["outPrefix"] + outStr
    ic(outFile)
    if outDict["saveFlag"]:
        mhwDset.to_netcdf(outFile)

def make_ord_array(inTimes):
    ''' Make ordinal time array required by e.g. mhws package '''
    tCftime = xr.cftime_range(inTimes[0], inTimes[len(inTimes)-1])
    missingTimes = list(set(inTimes).symmetric_difference(set(tCftime))) #Print missing timesteps between input and cfrange
    ic(missingTimes)
    ordList = list()
    for tcf in tCftime:
        dtAc = tcf
        dtAcOrd = date(dtAc.year, dtAc.month, dtAc.day).toordinal()
        ordList.append(dtAcOrd)
    ordArr = np.array(ordList)

    return ordArr
    
def mine_file_str(fileStr):
    ''' Mine useful metadata from filename '''
    pieces = fileStr.split('.')
    fileDict = {
        "scn": pieces[2],
        "rlz": pieces[6],
        "var": pieces[10],
        "times": pieces[11][:-3]
    }

    return fileDict
