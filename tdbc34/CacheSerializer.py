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
import pandas as pd

#constant variable
TicksTag = 100
BarsTag = 200
DoubleSize = 8
LongSize = 8

#Deserialize cTrader tdbc34 data file

    
def ReadInt32(fs:gzip.GzipFile):
    return int.from_bytes(fs.read(4),'little',signed='True')

def GetDateTime(ticks:int):
    """Convert .NET ticks to formatted ISO8601 time
    Args:
    ticks: integer
    i.e 100 nanosecond increments since 1/1/1 AD"""
    _date = datetime(1, 1, 1) + \
        timedelta(microseconds=ticks // 10)
    if _date.year < 1900:  # strftime() requires year >= 1900
        _date = _date.replace(year=_date.year + 1900)
    return _date
    # return _date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3]

def chunks(lst:str, n:int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def GetDouble(val:str):
    _out = struct.unpack('<d', val)[0]
    if math.isnan(_out):
        _out = None
    return _out


def GetLong(val:str):
    return struct.unpack('<q', val)[0]
    
def DeserializeTicks(fs:gzip.GzipFile):

    #read total record count in file
    rec_count = ReadInt32(fs)
    #read time array
    long_bytes = bytearray(rec_count * LongSize)
    fs.readinto(long_bytes)
    
    #read bid array
    bid_bytes = bytearray(rec_count * DoubleSize)
    fs.readinto(bid_bytes)
    
    #read ask array
    ask_bytes = bytearray(rec_count * DoubleSize)
    fs.readinto(ask_bytes)
    
    return pa.table(
        [
            pa.array([ GetDateTime(GetLong(_time)) for _time in chunks(long_bytes, LongSize)]),
            pa.array([GetDouble(bid) for bid in chunks(bid_bytes, DoubleSize)]),
            pa.array([GetDouble(ask) for ask in chunks(ask_bytes, DoubleSize)])
        ], schema = pa.schema([
            ("utctime", pa.timestamp('ms',tz="+0:0")),
            ("bid", pa.decimal128(15, 6)),
            ("ask", pa.decimal128(15, 6))
        ]) 
    )

    
def DeserializeBars(fs:gzip.GzipFile):

    #read total record count in file
    rec_count = ReadInt32(fs)
    #read time array
    long_bytes = bytearray(rec_count * LongSize)
    fs.readinto(long_bytes)

    #read open array
    open_bytes = bytearray(rec_count * DoubleSize)
    fs.readinto(open_bytes)

    #read high array
    high_bytes = bytearray(rec_count * DoubleSize)
    fs.readinto(high_bytes)

    #read low array
    low_bytes = bytearray(rec_count * DoubleSize)
    fs.readinto(low_bytes)

    #read close array
    close_bytes = bytearray(rec_count * DoubleSize)
    fs.readinto(close_bytes)

    #read tick volume array
    tickvol_bytes = bytearray(rec_count * LongSize)
    fs.readinto(tickvol_bytes)


    return pa.table([
        pa.array([GetDateTime(GetLong(_time)) for _time in chunks(long_bytes, LongSize)]),
        pa.array([GetDouble(_open) for _open in chunks(open_bytes, DoubleSize)]),
        pa.array([GetDouble(high) for high in chunks(high_bytes, DoubleSize)]),
        pa.array([GetDouble(low) for low in chunks(low_bytes, DoubleSize)]),
        pa.array([GetDouble(close) for close in chunks(close_bytes, DoubleSize)]),
        pa.array([GetLong(volume) for volume in chunks(tickvol_bytes, LongSize)])
    ], schema = pa.schema([
            ("utctime", pa.timestamp('ms',tz="+0:0")),
            ("open", pa.decimal128(15, 6)),
            ("high", pa.decimal128(15, 6)),
            ("low", pa.decimal128(15, 6)),
            ("close", pa.decimal128(15, 6)), 
            ("volume", pa.uint64())
        ]) 
    ) 

def OpenDataFile(filePath:str):
    data = {}

    with gzip.open(filePath, 'rb') as fs:
        tag = ReadInt32(fs) #read first 4 byte to determine data type

        if tag == TicksTag:    #100: Tick Data
            data = DeserializeTicks(fs)
        elif tag == BarsTag:   #200: Bar Data
            data = DeserializeBars(fs)
        else:
            raise Exception("tag data error. Unable to identify type") 

    return data
   
 

def DescribeCacheData(path:str,env="wsl"):

    if env=="wsl":
        _datapath = ("%s/*.tdbc34" % (path))
    elif env=="windows":
        _datapath = ("%s\\*.tdbc34" % (path))

    _allCachedFileNames = [Path(_name).stem for _name in glob.glob(_datapath)]
    _allCachedFileDates = [datetime.strptime(x, "%Y.%m.%d") for x in _allCachedFileNames]

    return (
        _allCachedFileNames,
        _allCachedFileDates
    )

def DescribeAvailableCacheData(env="wsl"):
    cache_locations = []
    cache_loc_pattern = ""
    _path_sep = ""
    if env=="wsl":
        cache_loc_pattern = ("/mnt/*/Users/*/AppData/Roaming/*/BacktestingCache/*/*/*")
        _path_sep = "/"
    elif env=="windows":
        home_directory = os.path.expanduser( '~' )
        cache_loc_pattern = ("%s\\AppData\Roaming\\*\\BacktestingCache\\*\\*\\*" % home_directory)
        _path_sep = "\\"
    else:
        raise Exception(("Not yet implemented environment '%s'" % env ))

    for path in glob.glob(cache_loc_pattern):

        _path_parts = path.split(sep=_path_sep)
        _account, _instrument, _cache_type  = _path_parts[-3:]
        
        # _datapath = ("%s\\*.tdbc34" % path)

        # _allCachedFileNames = [Path(_name).stem for _name in glob.glob(_datapath)]
        # _allCachedFileDates = [datetime.strptime(x, "%Y.%m.%d") for x in _allCachedFileNames]
        _allCachedFileNames, _allCachedFileDates = DescribeCacheData(path,env)

        if(len(_allCachedFileNames) ==0):
            print("%s - has zero files" % path)
            continue

        itm = {
            "account": _account,
            "instrument": _instrument,
            "type": _cache_type,
            "path": path,
            "from": min(_allCachedFileDates),
            "to": max(_allCachedFileDates)
        }

        cache_locations.append(itm)

    return cache_locations
    
def GetBackTestData(path="",start_date_str="",end_date_str="", env="wsl"):

    _allCachedFileNames, _allCachedFileDates = DescribeCacheData(path, env)

    if len(_allCachedFileNames) < 1:
        raise Exception(("No tdbc3 files in '%s'" % path ))

    _start_date = datetime.strptime(start_date_str, "%Y.%m.%d")
    _end_date = datetime.strptime(end_date_str, "%Y.%m.%d") + timedelta(days=1)

    # generate a range of date by search critieria
    _search_range = [datetime.fromordinal (d) for d in range(_start_date.toordinal(), _end_date.toordinal())]

    # generate an availble data range
    _available_range = sorted(set(_allCachedFileDates) & set(_search_range))

    # print(_available_range)
    paData = []
    if env=="wsl":
        _readPath = "%s/%s.tdbc34"
    elif env=="windows":
        _readPath = "%s\\%s.tdbc34" 

    for _date in _available_range:
        _date_str = _date.strftime("%Y.%m.%d")
        _cachePath = (_readPath % (path,_date_str))
        paData.append(OpenDataFile(_cachePath))

    return pa.concat_tables(paData)

def DescribeAvailableCacheDataInDataFrame(env="wsl"):
    return pd.json_normalize(DescribeAvailableCacheData(env))