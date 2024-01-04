import gzip
import glob
import math
import io
import sys
import struct
import pyarrow as pa
import os
from datetime import datetime
from datetime import timedelta
from pathlib import Path


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
  
    utc_times, bids, asks = [],[],[]
    
    while (rec_count < data_len):
        #read time
        utc_times += [GetDateTimeFromMilliseconds(GetLong(fs.read(LongSize)))]

        #read bid
        bids += [GetDouble(fs.read(DoubleSize))]

        #read ask
        asks += [GetDouble(fs.read(DoubleSize)) ]
        
        rec_count +=1
        
    return pa.table(
        [
            pa.array(utc_times),
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
    
    utc_times, opens, highs, lows, closes, volumes = [] ,[] ,[] ,[] ,[] , []

    while (rec_count < data_len):

        #read time
        utc_times += [GetDateTimeFromMilliseconds(GetLong(fs.read(LongSize)))]

        #read open
        opens += [GetDouble(fs.read(DoubleSize))]

        #read high
        highs += [GetDouble(fs.read(DoubleSize)) ]

        #read low
        lows += [GetDouble(fs.read(DoubleSize)) ]

        #read close
        closes += [GetDouble(fs.read(DoubleSize)) ]

        #read volume
        volumes += [GetLong(fs.read(LongSize))]
        
        rec_count +=1
    
    return pa.table([
        pa.array(utc_times),
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
        cache_loc_pattern = ("/mnt/*/Users/*/AppData/Roaming/????????/Cache/*/BacktestingCache/*/*/[mt]1")
        _path_sep = "/"
    elif env=="windows":
        home_directory = os.path.expanduser( '~' )
        cache_loc_pattern = ("%s\\AppData\Roaming\\????????\\Cache\\*\\BacktestingCache\\*\\*\\[mt]1" % home_directory)
        _path_sep = "\\"
    else:
        raise Exception(("Not yet implemented environment '%s'" % env ))

    for path in glob.glob(cache_loc_pattern):

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
    return pd.json_normalize(DescribeAvailableCacheData(env))