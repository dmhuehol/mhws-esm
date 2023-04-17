''' mending
Mend shards back into global dataset.

#### INPUTS ####
dataDict: defines input data
    dataPath: input path of data files to be shattered
    dataToken: suggested tokens for datasets below
        ARISE-SAI-1.5: '*SSP245-TSMLT-GAUSS*'
        SSP2-4.5: '*BWSSP245*'
outDict: specify features of output data
    savePath: path to save shard files
    outPrefix: optional prefix to append to output file names

Written by Daniel Hueholt
Graduate Research Assistant at Colorado State University 
'''
import glob
import sys

from icecream import ic
import numpy as np
import xarray as xr

import fun_mhws as fm

##### INPUTS: SEE DOCSTRING FOR DOCUMENTATION ####
dataDict = { 
    # "dataPath": '/Users/dhueholt/Documents/mhwwg_out/',
    "dataPath": '/glade/scratch/dhueholt/mhws_out/shards/',
    "dataToken": '*BWSSP245*'
}
outDict = {
    "saveFlag": True,
    # "savePath": '/Users/dhueholt/Documents/mhwwg_out/mending/',
    "savePath": '/glade/scratch/dhueholt/mhws_out/mending/',
    "outPrefix": ''
}
memberTokens = list(
    ['*_001_*', '*_002_*', '*_003_*', '*_004_*', '*_005_*', '*_006_*', '*_007_*',
    '*_008_*', '*_009_*', '*_010_*'])

for m in memberTokens:
    inToken = dataDict["dataToken"] + m
    inStr = dataDict["dataPath"] + inToken
    try:
        mendDs = xr.open_mfdataset(inStr, chunks={'lat': 64}) #automatically concatenates along lat
        shardGlobs = sorted(glob.glob(inStr))
        if shardGlobs != []:
            strWithShard = shardGlobs[0].split('/')[-1]
            shardLat = strWithShard.split('_')[-1][:-3]
            outStr = strWithShard.replace('_'+shardLat,'')
            outMendFile = outDict["savePath"] + outDict["outPrefix"] + outStr
            ic(outMendFile)
            if outDict["saveFlag"]:
                mendDs.to_netcdf(outMendFile)
    except: # Usually means no data
        pass