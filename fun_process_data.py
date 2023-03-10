

def call_to_open(dataDict, setDict):
    ''' Open data and carry out tasks common to all types '''
    lf = col.defaultdict(list) # List factory stackoverflow.com/a/2402678
    # Open datasets for each input experiment
    for dky in dataDict.keys():
        if 'id' in dky:
            try:
                inPath = dataDict["dataPath"] + dataDict[dky]
                inGlobs = sorted(glob.glob(inPath))
                lf['globs'].append(inGlobs) # Used to track realizations
                rawDset = xr.open_mfdataset(
                    inPath, concat_dim='realization', 
                    combine='nested', coords='minimal')
                try:
                    sourceId = rawDset.attrs['source_id']
                except:
                    sourceId = None
                dataKey = discover_data_var(rawDset)
                rawDarr = rawDset[dataKey]
                scnDarr = bind_scenario(rawDarr, dataDict[dky])
                if scnDarr.scenario == 'PreindustrialControl': 
                # TODO: make this elegant for all
                    scnDarr.attrs['scenario'] = sourceId + ':' + scnDarr.scenario
                maskDarr = apply_mask(scnDarr, dataDict, setDict)
                if 'CESM2-ARISE:Control' in scnDarr.scenario: # Two parts: historical and future
                    lf['chf'].append(maskDarr) # Keep separate for now
                else:
                    lf['darr'].append(maskDarr) # Put other data in darrList
            except Exception as fileOpenErr: # Reached for None input
                ic(fileOpenErr) # Display reason for error
                pass # Move on if possible

    # Handle the two parts of the CESM2-ARISE Control                
    if len(lf['chf']) == 2: # Hist & future input
        acntrlDarr = combine_hist_fut(lf['chf'][0], lf['chf'][1])
        lf['darr'].append(acntrlDarr)
        acntrlDarr.attrs['scenario'] = 'CESM2-WACCM/ARISE:Control/SSP2-4.5'
    else: # Either hist OR future input
        try:
            lf['darr'].append(lf['chf'][0]) # Use whichever is present
        except:
            pass # If there is no data, move on if possible
            
    if len(lf['darr']) == 0:
        raise CustomExceptions.NoDataError(
            'No data! Check input and try again.')

    # Manage ensemble members
    for ec in lf['globs']:
        lf['emem'].append(get_ens_mem(ec))
    lf['emem'] = list(filter(None, lf['emem']))
    for dc,darr in enumerate(lf['darr']):
        scnArr, ememStr = manage_realizations(
            setDict, darr, lf['emem'][dc])
        lf['scn'].append(scnArr)
        lf['ememStr'].append(ememStr)
    lf['ememStr'] = list(filter(None,lf['ememStr']))
    ememSave = '-'.join(lf['ememStr'])
    cmnDict = {'dataKey': dataKey, 'ememSave': ememSave}
    
    # Convert units and calculate variables
    if setDict["convert"] is not None:
        for fcufcv in setDict["convert"]:
            for rc,rv in enumerate(lf['scn']):
                try:
                    lf['scn'][rc] = fcufcv(rv) # Apply converter or calculator function(s)
                except:
                    lf['scn'][rc] = fcufcv(rv, setDict) # Sometimes they need setDict
    
    return lf['scn'], cmnDict