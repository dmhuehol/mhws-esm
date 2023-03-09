''' fun_derive_data
Functions to derive and save data from a base dataset as a new file.

Written by Daniel Hueholt
Graduate Research Assistant at Colorado State University
'''

from icecream import ic
import sys

from cftime import DatetimeNoLeap as dtnl
import cftime
from datetime import date
import marineHeatWaves as mhws
import numpy as np
import xarray as xr

def derive_mhw_presence(inFileSst, outPath):
    ''' Calculate marine heatwave presence from daily SST data using the Hobday
    et al. 2016 definition. Returns binary classification (1=present 0=absent).
    Intensity data is not saved. Calculation relative to FIXED BASELINE:
    2010-2019 for GLENS or ARISE-SAI. Baseline definition file MUST be
    calculated first. Rolling sum is left aligned by default.'''
    mhwDefDict = {
        "defPath": '/Users/dhueholt/Documents/GLENS_data/extreme_MHW/definitionFiles/',
        "defPathCasper": '/glade/work/dhueholt/definitionFiles/',
        "defFile": 'mhwDefsFile_GLENS_WAusMHW-30_628N112_5E.nc',
        "defKey": 'mn_SST'
    }
    ic(mhwDefDict["defFile"]) #Helps ensure the correct definitions file is used
    annSum = 5
    inKey = 'SST'
    outKey = 'binary_mhw_pres'
    regOfInt = rlib.WesternAustraliaMHW_point() #Use point location in almost all circumstances

    # Load data
    sstDset = xr.open_dataset(inFileSst)
    sstDarr = sstDset[inKey]
    sstReg, locStr, _ = fpd.manage_area(sstDarr, regOfInt, areaAvgBool=True)
    sstRegFullTimes = sstReg.resample(time='1D').asfreq() #Add missing timesteps with NaN value
    sstRegDat = sstRegFullTimes.data.squeeze()
    times = sstRegFullTimes.time.data
    ordArr = fpd.make_ord_array(times)

    # Load baseline definition file
    mhwDef = xr.open_dataset(mhwDefDict["defPath"] + mhwDefDict["defFile"])
    mhwDefSst = mhwDef[mhwDefDict["defKey"]].data
    mhwDefTimes = mhwDef.time.data
    altClim = list([mhwDefTimes, mhwDefSst]) #Format required by mhws alternateClimatology feature

    mhwsDict, climDict = mhws.detect(ordArr, sstRegDat, climatologyPeriod=[2010,2019], alternateClimatology=altClim)

    # Make binary MHW presence/absence array
    binMhwPres = np.zeros(np.shape(ordArr)) #Initiate blank array of the proper size
    mhwStrtInd = mhwsDict['index_start']
    mhwDurInd = mhwsDict['duration']
    for actInd,startInd in enumerate(mhwStrtInd):
        actDur = mhwDurInd[actInd] #Duration of active MHW
        binMhwPres[startInd:startInd+actDur] = 1 #Times with active MHW get a 1

    # Make Dataset with MHW data
    newDset = xr.Dataset(
        data_vars=dict(a=(["time"], binMhwPres)),
        coords=dict(time=times),
        attrs=dict(description='MHW data')
    )
    newDset = newDset.rename_vars({'a': outKey})
    newDset[outKey].attrs['long_name'] = 'Binary presence-absence of MHWs'

    inPcs = inFileSst.split('/') #inFilePrect is the entire path to file
    inFn = inPcs[len(inPcs)-1] #Filename is the last part of the path
    outKeyReg = 'binary_mhw' + locStr
    strOut = inFn.replace(inKey, outKeyReg) #Replace var name with extreme key
    outFile = outPath + strOut

    if annSum == True:
        ic(annSum)
        newDset = newDset.groupby("time.year").sum()
        outFile = outFile.replace('.nc', 'ann.nc')
        timeList = list()
        for yr in newDset.year:
            timeList.append(dtnl(yr,7,15,12,0,0,0)) #Year with standard fill values
        outDset = xr.Dataset(
            data_vars=dict(
                a=(["time"], newDset[outKey].data)
            ),
            coords=dict(time=timeList),
            attrs=dict(description='MHW data')
        )
        outDset = outDset.rename_vars({'a': outKey})
        outDset[outKey].attrs['long_name'] = 'Binary presence-absence of MHWs'
    elif isinstance(annSum, int):
        ic('rolling', annSum)
        newDset = newDset.groupby("time.year").sum()
        newDset = newDset.rolling(year=annSum, center=False).sum().shift(year=1-annSum) #Rolling sum
        outFile = outFile.replace('.nc', 'roll' + str(annSum) + '.nc')
        timeList = list()
        for yr in newDset.year:
            timeList.append(dtnl(yr,7,15,12,0,0,0)) #Year with standard fill values
        outDset = xr.Dataset(
            data_vars=dict(
                a=(["time"], newDset[outKey].data)
            ),
            coords=dict(time=timeList),
            attrs=dict(description='MHW data')
        )
        outDset = outDset.rename_vars({'a': outKey})
        outDset[outKey].attrs['long_name'] = 'Binary presence-absence of MHWs'
        outDset.to_netcdf(outFile) #Save data
    else:
        outDset = newDset.copy()

    outDset.to_netcdf(outFile) #Save data
    ic(strOut, outFile, outDset) #icecream all useful parts of the output
