''' fun_mhws
Functions used for calculating MHWs and preprocessing data in the
mhws-esm package.

Written by Daniel Hueholt
Graduate Research Assistant at Colorado State University
'''
from datetime import date
import sys
import time

import dask
from icecream import ic
import marineHeatWaves as mhws
import numpy as np
import xarray as xr
xr.set_options(keep_attrs=True)

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
    templateShape = np.shape(mhwTemplate)
    # By definition, there can never be more MHW events than timesteps/7. This reduces the 
    # size of the output while still providing sufficient padding to calculate events.
    timeStrtTemplate = np.empty(
        (int(templateShape[0]/7), int(templateShape[1]), int(templateShape[2]))) # "event"xlatxlon
    timeStrtTemplate.fill(np.nan)
    timeEndTemplate = np.copy(timeStrtTemplate) # "event"xlatxlon
    mhwDurTemplate = np.copy(timeStrtTemplate) # "event"xlatxlon
    mhwMeanIntTemplate = np.copy(timeStrtTemplate) # "event"xlatxlon
    mhwCatTemplate = np.copy(timeStrtTemplate) # "event"xlatxlon
    
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
                mhwDur = mhwsDict['duration']
                for actInd,startInd in enumerate(mhwStrtInd):
                    actDur = mhwDur[actInd]
                    bnryMhw[startInd:startInd+actDur] = 1 # Times with active MHW get a 1
                    # MHW properties are padded to be consistent along "event" dimension
                    timeStrtTemplate[actInd, latc, lonc] = mhwsDict["time_start"][actInd]
                    timeEndTemplate[actInd, latc, lonc] = mhwsDict["time_end"][actInd]
                    mhwDurTemplate[actInd, latc, lonc] = actDur
                    mhwMeanIntTemplate[actInd, latc, lonc] = mhwsDict["intensity_mean"][actInd]
                    actCat = mhwsDict["category"][actInd]
                    if actCat == 'Moderate':
                        mhwCatTemplate[actInd, latc, lonc] = 1
                    elif actCat == 'Strong':
                        mhwCatTemplate[actInd, latc, lonc] = 2
                    elif actCat == 'Severe':
                        mhwCatTemplate[actInd, latc, lonc] = 3
                    elif actCat == 'Extreme':
                        mhwCatTemplate[actInd, latc, lonc] = 4
                    else:
                        ic(actCat)
                        mhwCatTemplate[actInd, latc, lonc] = -9999
                    
                mhwTemplate[:, latc, lonc] = bnryMhw
        
                # tocEachMhw = time.time() - ticEachMhw #; ic(tocEachMhw)
                # timerEachMhw.append(tocEachMhw)           
    # tocMhwOverall = time.time() - ticMhwOverall; ic(tocMhwOverall)
    # ic(timerJustCalc, np.mean(timerJustCalc))
    # ic(timerEachMhw, np.mean(timerEachMhw))
    # ic(np.unique(mhwTemplate))
    
    # Construct MHWs datasets and save to netCDF
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
        
    mhwPropsDset = xr.Dataset(
        data_vars=dict(
            time_start=(["event", "lat", "lon"], timeStrtTemplate),
            time_end=(["event", "lat", "lon"], timeEndTemplate),
            duration=(["event", "lat", "lon"], mhwDurTemplate),
            mean_intensity=(["event", "lat", "lon"], mhwMeanIntTemplate),
            category=(["event", "lat", "lon"], mhwCatTemplate),
        ),
        coords=dict(
            event=np.arange(0,len(mhwDurTemplate)),
            lat=rawDset.lat,
            lon=rawDset.lon
        ),
        attrs=dict(description="Marine heatwave properties"),
    )
    mhwPropsDset['time_start'].attrs = rawDset.attrs
    mhwPropsDset['time_start'].attrs['long_name'] = 'Start time of MHWs'
    mhwPropsDset['time_end'].attrs = rawDset.attrs
    mhwPropsDset['time_end'].attrs['long_name'] = 'End time of MHWs'
    mhwPropsDset['duration'].attrs = rawDset.attrs
    mhwPropsDset['duration'].attrs['long_name'] = 'Duration of MHWs'
    mhwPropsDset['mean_intensity'].attrs = rawDset.attrs
    mhwPropsDset['mean_intensity'].attrs['long_name'] = 'Mean intensity of MHWs'
    mhwPropsDset['category'].attrs = rawDset.attrs
    mhwPropsDset['category'].attrs['long_name'] = 'Category of MHWs'
    
    outStrProps = dataFile.split('/')[-1].replace('SST', 'mhwProps')
    outFileProps = outDict["outPath"] + outDict["outPrefix"] + outStrProps
    ic(outFileProps)
    if outDict["saveFlag"]:
        mhwPropsDset.to_netcdf(outFileProps)
        

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
    # b.e21.BW.f09_g17.SSP245-TSMLT-GAUSS-DEFAULT.001.pop.h.nday1.SST.20350101-20691231_RG.nc
    pieces = fileStr.split('.')
    if 'BWSSP245' in fileStr: #no-SAI SSP2-4.5
        fileDict = {
            "scn": pieces[2],
            "rlz": pieces[6],
            "var": pieces[10],
            "times": pieces[11][:-3]
        }
    elif 'SSP245-TSMLT-GAUSS-DEFAULT' in fileStr: #SAI ARISE-SAI-1.5
        fileDict = {
            "scn": pieces[4],
            "rlz": pieces[5],
            "var": pieces[9],
            "times": pieces[10][:-3]
        }

    return fileDict
