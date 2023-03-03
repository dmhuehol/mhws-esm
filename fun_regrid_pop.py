''' fun_regrid_pop
Functions to regrid Parallel Ocean Model (POP) output from the B-grid to a
standard lat/lon using interpolation.

Originally written by Emily Gordon based on code provided by Zachary Labe
Modified by Daniel Hueholt
'''

from icecream import ic
import sys

import numpy as np
import xarray as xr
import scipy.interpolate as interp

def extract_ocn_latlons(ocngrid, latName, lonName):
    ''' Extract lat/lons from an ocean grid (POP, NEMO) '''
    ocnLat = np.asarray(ocngrid[latName].data)
    ocnLon = np.asarray(ocngrid[lonName].data)

    return ocnLat, ocnLon

def regrid(dataIn, latIn, lonIn, latOut, lonOut):
    ''' Takes POP model output on a B-grid (T-cells for scalars,
    U-cells for vectors) (latInxlonIn) or NEMO output on a C-grid
    (i,j coordinates) and regrids to (latOutxlonOut) '''
    dataRv = np.ravel(dataIn)   
    dataRegrid = interp.griddata(
        (latIn,lonIn), dataRv, (latOut,lonOut), method='linear')

    return dataRegrid

def operate_regrid_cesm(popDataArray, popLat, popLon):
    ''' Carry out regridding from CESM input (POP) '''
    varOfInt = np.squeeze(popDataArray.data)
    time = popDataArray.time
    latNew = np.arange(-90, 91, 0.94240838)
    latNew = latNew[:-1]
    lonNew = np.arange(0, 360, 1.25)
    # popLat[popLat>90] = np.nan
    # popLon[popLon>360] = np.nan

    dataRegrid = np.empty(
        (varOfInt.shape[0], latNew.shape[0], lonNew.shape[0]))
    
    lonOut,latOut = np.meshgrid(lonNew, latNew) # make grid
    lonRg = np.ravel(popLon)
    latRg = np.ravel(popLat)
    for bc in range(varOfInt.shape[0]):
        newDat = regrid(
            varOfInt[bc], latRg, lonRg, latOut, lonOut)
        dataRegrid[bc] = newDat

    dataKey = popDataArray.name
    newDset = xr.Dataset(
        {dataKey: (("time","lat","lon"), dataRegrid)},
        coords={
            "time": time,
            "lat": latNew,
            "lon": lonNew
        }
    )
    newDset[dataKey].attrs = popDataArray.attrs
    
    strOut = popDataArray.attrs['inFile'].replace(".nc","_RG.nc")
    outFile = popDataArray.attrs['outPath'] + strOut
    newDset.to_netcdf(outFile)
    ic(strOut, outFile, newDset)

def operate_regrid_ukesm(ocnDataArray, ocnLat, ocnLon):
    ''' Carry out regridding from UKESM input (NEMO) '''
    varOfInt = np.squeeze(ocnDataArray.data)
    time = ocnDataArray.time
    latNew = np.arange(-89.375, 90, 1.25)
    lonNew = np.arange(-180, 180, 1.875)
    # lonNew360 = np.arange(0.9375, 360, 1.875)

    dataRegrid = np.empty((varOfInt.shape[0], latNew.shape[0], lonNew.shape[0]))

    lonOut,latOut = np.meshgrid(lonNew, latNew) # make grid
    lonRv = np.ravel(ocnLon)
    latRv = np.ravel(ocnLat)
    for bc in range(varOfInt.shape[0]):
        newDat = regrid(
            varOfInt[bc], latRv, lonRv, latOut, lonOut)
        dataRegrid[bc] = newDat

    dataKey = ocnDataArray.name
    newDset = xr.Dataset(
        {dataKey: (("time","lat","lon"), dataRegrid)},
        coords={
            "time": time,
            "lat": latNew,
            "lon": lonNew
        }
    )
    newDset[dataKey].attrs = ocnDataArray.attrs

    strOut = ocnDataArray.attrs['inFile'].replace(".nc","_RG.nc")
    outFile = ocnDataArray.attrs['outPath'] + strOut
    newDset.to_netcdf(outFile)
    ic(strOut, outFile, newDset)