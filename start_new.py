'''start_new 
Totally fresh beginning to just open netCDF files and shatter them into 5
deg latitude pieces. '''

from icecream import ic
import sys
import time

import numpy as np
import xarray as xr

dataDict = {
    "dataPath": '/Users/dhueholt/Documents/GLENS_data/daily_SST/GLENS_defsPeriod/',
    "idGlensCntrl": '*control*', #'*control*' or None
    "idGlensFdbck": '*feedback*', #'*feedback*' or None
    "idArise": None, #'*SSP245-TSMLT-GAUSS*' or None
    "idS245Cntrl": None, # '*BWSSP245*' or None
    "idS245Hist": None, #'*BWHIST*' or None
    "idTest": 'b.e15.B5505C5WCCML45BGCR.f09_g16.control.001.pop.h.nday1.SST.20100102-20191231_RG.nc', # For writing
    "mask": '/Users/dhueholt/Documents/Summery_Summary/cesm_atm_mask.nc'
}

file = dataDict["dataPath"] + dataDict["idTest"]
tic = time.time()
inDset = xr.open_dataset(file)
tocOpen = time.time() - tic
ic(tocOpen)
ic(inDset.lat)
slices = (slice(0,90), slice(-90,0))
sliceUp = inDset.sel(lat = slices[0])
sliceDown = inDset.sel(lat = slices[1])
ic(sliceUp)
ic(sliceDown)
ic(sliceUp.lat)
ic(sliceDown.lat)