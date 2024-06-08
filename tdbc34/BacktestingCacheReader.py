import gzip
import glob
import gc
import struct
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import numpy as np
from multiprocessing import Pool
import os
from datetime import datetime
from datetime import timedelta
from pathlib import Path
import pandas as pd

#constant variable
DoubleSize = 8
LongSize = 8

#Deserialize cTrader zbars/zticks data file

def GetDateTimeFromMilliseconds(ms:int):
    return datetime.fromtimestamp(ms/1000.0)

def GetDouble(val:str):
    return GetLong(val)/100000.0

def GetLong(val:str):
    return struct.unpack('<q', val)[0]

def GetFileLength(fs:gzip.GzipFile):
    
    #find file size
    fs.seek(0, 2)
    file_len = fs.tell()
    #reset stream position
    fs.seek(0)
    
    return file_len
    
def DeserializeTicks(fs:gzip.GzipFile):

    #read total record count in file
    rec_count = 0
    
    data_len = GetFileLength(fs) / 24 #24 = 8 bytes * 3 elements (timestamp, bid, ask)
  
    utc_times, bids, asks = np.array([]), np.array([]),np.array([])
    
    while (rec_count < data_len):
        #read time
        utc_times = np.append(utc_times, GetDateTimeFromMilliseconds(GetLong(fs.read(LongSize))))


        #read bid
        bids = np.append(bids, GetDouble(fs.read(DoubleSize)))

        #read ask
        asks = np.append(asks, GetDouble(fs.read(DoubleSize)))
        
        rec_count +=1
    
    gc.collect()

    return pa.table(
        [
            pa.array(utc_times.astype('datetime64[ms]')),
            pa.array(bids),
            pa.array(asks)
        ], schema = pa.schema([
            ("utctime", pa.timestamp('ms',tz="UTC")),
            ("bid", pa.decimal128(15, 6)),
            ("ask", pa.decimal128(15, 6))
        ])
    ) 
    

def DeserializeBars(fs:gzip.GzipFile):
    
    rec_count = 0
    
    data_len = GetFileLength(fs) / 48 #48 = 8bytes * 6 elements (timestamp, open, high, low, close, volume)
    
    utc_times, opens, highs, lows, closes, volumes = np.array([]), np.array([]),np.array([]),np.array([]),np.array([]),np.array([])
    
    while (rec_count < data_len):

        #read time
        utc_times = np.append(utc_times, GetDateTimeFromMilliseconds(GetLong(fs.read(LongSize))))

        #read open
        opens = np.append(opens, GetDouble(fs.read(DoubleSize)))

        #read high
        highs = np.append(highs, GetDouble(fs.read(DoubleSize)))

        #read low
        lows = np.append(lows, GetDouble(fs.read(DoubleSize)))

        #read close
        closes = np.append(closes, GetDouble(fs.read(DoubleSize)))

        #read volume
        volumes = np.append(volumes, GetLong(fs.read(LongSize)))
        
        rec_count +=1
    
    gc.collect()
    
    return pa.table([
        pa.array(utc_times.astype('datetime64[ms]')),
        pa.array(opens),
        pa.array(highs),
        pa.array(lows),
        pa.array(closes),
        pa.array(volumes)
    ], schema = pa.schema([
            ("utctime", pa.timestamp('ms',tz="UTC")),
            ("open", pa.decimal128(15, 6)),
            ("high", pa.decimal128(15, 6)),
            ("low", pa.decimal128(15, 6)),
            ("close", pa.decimal128(15, 6)), 
            ("volume", pa.uint64())
        ]) 
    )
   
def OpenDataFile(filePath:str):
    data = {}
    
    file_type = Path(filePath).suffix

    with gzip.open(filePath, 'rb') as fs:
       
        if file_type == ".zticks": 
            data = DeserializeTicks(fs)
        elif file_type == ".zbars": 
            data = DeserializeBars(fs)
        else:
            raise Exception("tag data error. Unable to identify type") 

    return data
   
 

def DescribeCacheData(path:str,env="wsl"):

    if env=="wsl":
        _datapath = ("%s/*.z*s" % (path))
    elif env=="windows":
        _datapath = ("%s\\*.z*s" % (path))
    
    return dict(
        (datetime.strptime(Path(_name).stem, "%Y%m%d"), Path(_name).name) 
        for _name in glob.glob(_datapath) 
    )


def DescribeAvailableCachedData(env="wsl"):
    cache_locations = []
    cache_loc_pattern = ""
    _path_sep = ""
    if env=="wsl":
        # cache_loc_pattern = ("/mnt/*/Users/*/AppData/Roaming/**/BacktestingCache/*/*/[mt]1")
        cache_loc_pattern = ("/mnt/*/Users/*/AppData/Roaming/????????/Cache/*/BacktestingCache/**/[mt]1")
        _path_sep = "/"
    elif env=="windows":
        home_directory = os.path.expanduser( '~' )
        cache_loc_pattern = ("%s\\AppData\Roaming\\????????\\Cache\\*\\BacktestingCache\\**\\[mt]1" % home_directory)
        _path_sep = "\\"
    else:
        raise Exception(("Not yet implemented environment '%s'" % env ))

    for path in glob.glob(cache_loc_pattern, recursive = True):

        _path_parts = path.split(sep=_path_sep)
        _account, _instrument, _cache_type  = _path_parts[-3:]
        
        _allCachedFileNames = DescribeCacheData(path,env)

        if(len(_allCachedFileNames) ==0):
            print("%s - has zero files" % path)
            continue

        itm = {
            "account": _account,
            "instrument": _instrument,
            "type": _cache_type,
            "path": path,
            "from": min(_allCachedFileNames.keys()),
            "to": max(_allCachedFileNames.keys())
        }

        cache_locations.append(itm)

    return cache_locations
    
def GetCachedBackTestData(path="",start_date_str="",end_date_str="", env="wsl"):

    _allCachedFileNames = DescribeCacheData(path, env)

    if len(_allCachedFileNames) < 1:
        raise Exception(("No chache file files in '%s'" % path ))

    _start_date = datetime.strptime(start_date_str, "%Y%m%d")
    _end_date = datetime.strptime(end_date_str, "%Y%m%d") + timedelta(days=1)

    # generate a range of date by search critieria
    _search_range = [datetime.fromordinal (d) for d in range(_start_date.toordinal(), _end_date.toordinal())]

    # generate an availble data range
    _available_range = sorted(set(_allCachedFileNames.keys()) & set(_search_range))

    # print(_available_range)
    paData = []
    if env=="wsl":
        _readPath = "%s/%s"
    elif env=="windows":
        _readPath = "%s\\%s" 

    for _date in _available_range:
        _cachePath = (_readPath % (path,_allCachedFileNames[_date]))
        # print(_cachePath)
        paData.append(OpenDataFile(_cachePath))

    return pa.concat_tables(paData)

def DescribeAvailableCacheDataInDataFrame(env="wsl"):
    pd.set_option('display.max_colwidth', None)
    return pd.json_normalize(DescribeAvailableCachedData(env))


def ConvertToParquet(_datafile_path,_dest_path,):
    paData = OpenDataFile(_datafile_path)

    #Create partition key
    paData = paData.add_column(0,"yearmon", [
        pc.strftime(paData['utctime'],format="%Y%m")
    ])

    _datafilename = Path(_datafile_path).stem

    print(("processing data file %s" % _datafilename))

    ds.write_dataset(
        data = paData,
        base_dir = _dest_path,
        format= "parquet",
        partitioning=ds.partitioning(
            pa.schema([("yearmon",pa.string())])
        ),
        basename_template="part-{0}-{{i}}.parquet".format(_datafilename),
        existing_data_behavior='overwrite_or_ignore'
    )

def LoadCacheToParquetDatasets(env="wsl",cache_location="~/.tdbc34/"):
    _allCachedData = DescribeAvailableCachedData(env)
    for itm in _allCachedData:

        print(("processing instrument %s" % itm['instrument']))

        _allCachedFileNames = DescribeCacheData(itm['path'], env)

        if len(_allCachedFileNames) < 1:
            print("No chache file files in '%s'" % itm['path'] )
            continue
        
        _dest_path = os.path.join(cache_location, itm['account'], itm['instrument'], itm['type'])

        with Pool() as pool:
            result  = [
                pool.apply(ConvertToParquet, args=(
                    os.path.join(itm['path'],_allCachedFileNames[k]),
                    _dest_path,
                    )
                    ) for k in _allCachedFileNames]
            
          
