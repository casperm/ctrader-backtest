# Read tdbc34 backtest files with Windows WSL

This Python library deserialize tdbc34 cache files from your cTrader Backtesting Cach director.

## Installation
```bash
pip install git+https://github.com/casperm/ctrader-backtest.git
```

## Import library
```python
from tdbc34 import CacheSerializer as cache
```

## List all available cache files.
Run ***DescribeAvailableCacheData*** function use glob regex command to match the cache location.
Parameter: env
Options: wsl (default) - Windows Subsystem for Linux


```python
cache.DescribeAvailableCacheData()[-4::]
```

    /mnt/z/Users/Home/AppData/Roaming/XXXXXXXXXXX/BacktestingCache/XXXXXXXXX/USDCHF/Ticks - has zero files
    /mnt/z/Users/Home/AppData/Roaming/XXXXXXXXXXX/BacktestingCache/XXXXXXXXX/USDJPY/Ticks - has zero files


    [{'account': 'demo_456789',
      'instrument': 'USDCAD',
      'type': 'Hour',
      'path': '/mnt/z/Users/Home/AppData/Roaming/XXXXXXXXXXX/BacktestingCache/XXXXXXXXX/USDCAD/Hour',
      'from': datetime.datetime(2013, 7, 1, 0, 0),
      'to': datetime.datetime(2013, 7, 1, 0, 0)},
     {'account': 'demo_456789',
      'instrument': 'USDCAD',
      'type': 'Ticks',
      'path': '/mnt/z/Users/Home/AppData/Roaming/XXXXXXXXXXX/BacktestingCache/XXXXXXXXX/USDCAD/Ticks',
      'from': datetime.datetime(2013, 7, 22, 0, 0),
      'to': datetime.datetime(2019, 9, 1, 0, 0)},
     {'account': 'demo_456789',
      'instrument': 'USDCHF',
      'type': 'Hour',
      'path': '/mnt/z/Users/Home/AppData/Roaming/XXXXXXXXXXX/BacktestingCache/XXXXXXXXX/USDCHF/Hour',
      'from': datetime.datetime(2013, 7, 1, 0, 0),
      'to': datetime.datetime(2013, 7, 1, 0, 0)},
     {'account': 'demo_456789',
      'instrument': 'USDJPY',
      'type': 'Hour',
      'path': '/mnt/z/Users/Home/AppData/Roaming/XXXXXXXXXXX/BacktestingCache/XXXXXXXXX/USDJPY/Hour',
      'from': datetime.datetime(2013, 7, 1, 0, 0),
      'to': datetime.datetime(2013, 7, 1, 0, 0)}]



Run ***DescribeAvailableCacheDataInDataFrame*** function to list cache location in ***Pandas DataFrame***


```python
cache.DescribeAvailableCacheDataInDataFrame()
```

    /mnt/z/Users/Home/AppData/Roaming/XXXXXXXXXXX/BacktestingCache/XXXXXXXXX/USDCHF/Ticks - has zero files
    /mnt/z/Users/Home/AppData/Roaming/XXXXXXXXXXX/BacktestingCache/XXXXXXXXX/USDJPY/Ticks - has zero files


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>account</th>
      <th>instrument</th>
      <th>type</th>
      <th>path</th>
      <th>from</th>
      <th>to</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>demo_12345678</td>
      <td>EURUSD</td>
      <td>Minute</td>
      <td>/mnt/z/Users/Home/AppData/Roaming/XXXXXXXXX...</td>
      <td>2019-12-28</td>
      <td>2019-12-31</td>
    </tr>
    <tr>
      <th>1</th>
      <td>demo_12345678</td>
      <td>EURUSD</td>
      <td>Ticks</td>
      <td>/mnt/z/Users/Home/AppData/Roaming/XXXXXXXXX...</td>
      <td>2019-12-31</td>
      <td>2020-01-13</td>
    </tr>
    <tr>
      <th>2</th>
      <td>demo_456789</td>
      <td>AUDJPY</td>
      <td>Hour</td>
      <td>/mnt/z/Users/Home/AppData/Roaming/XXXXXXXXX...</td>
      <td>2011-01-01</td>
      <td>2013-07-01</td>
    </tr>
    <tr>
      <th>3</th>
      <td>demo_456789</td>
      <td>AUDJPY</td>
      <td>Minute</td>
      <td>/mnt/z/Users/Home/AppData/Roaming/XXXXXXXXX...</td>
      <td>2011-02-04</td>
      <td>2020-08-01</td>
    </tr>
    <tr>
      <th>4</th>
      <td>demo_456789</td>
      <td>AUDJPY</td>
      <td>Ticks</td>
      <td>/mnt/z/Users/Home/AppData/Roaming/XXXXXXXXX...</td>
      <td>2013-07-22</td>
      <td>2020-08-01</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>61</th>
      <td>demo_456789</td>
      <td>US500</td>
      <td>Ticks</td>
      <td>/mnt/z/Users/Home/AppData/Roaming/XXXXXXXXX...</td>
      <td>2015-01-23</td>
      <td>2022-10-04</td>
    </tr>
    <tr>
      <th>62</th>
      <td>demo_456789</td>
      <td>USDCAD</td>
      <td>Hour</td>
      <td>/mnt/z/Users/Home/AppData/Roaming/XXXXXXXXX...</td>
      <td>2013-07-01</td>
      <td>2013-07-01</td>
    </tr>
    <tr>
      <th>63</th>
      <td>demo_456789</td>
      <td>USDCAD</td>
      <td>Ticks</td>
      <td>/mnt/z/Users/Home/AppData/Roaming/XXXXXXXXX...</td>
      <td>2013-07-22</td>
      <td>2018-10-23</td>
    </tr>
    <tr>
      <th>64</th>
      <td>demo_456789</td>
      <td>USDCHF</td>
      <td>Hour</td>
      <td>/mnt/z/Users/Home/AppData/Roaming/XXXXXXXXX...</td>
      <td>2013-07-01</td>
      <td>2013-07-01</td>
    </tr>
    <tr>
      <th>65</th>
      <td>demo_456789</td>
      <td>USDJPY</td>
      <td>Hour</td>
      <td>/mnt/z/Users/Home/AppData/Roaming/XXXXXXXXX...</td>
      <td>2013-07-01</td>
      <td>2013-07-01</td>
    </tr>
  </tbody>
</table>
<p>66 rows × 6 columns</p>
</div>



## Load Cached files 
The ***GetBackTestData*** takes three parameters (cacheFiles, start_date, end_date). And returns [PyArrow](https://arrow.apache.org/docs/python/index.html) datatable.


```python
%%time
_cacheFiles = "/mnt/z/Users/Home/AppData/Roaming/XXXXXXXXXXX/BacktestingCache/XXXXXXXXX/US500/Ticks"
US500Ticks = cache.GetBackTestData(_cacheFiles,"2021.01.01","2021.03.01")
```

    CPU times: user 2.47 s, sys: 45.4 ms, total: 2.51 s
    Wall time: 3.3 s


## Query PyArrow datatable with SQL ([DuckDB](https://duckdb.org/) OLAP Engine)


```python
import duckdb
import pandas as pd
import sqlalchemy
# No need to import duckdb_engine
#  SQLAlchemy will auto-detect the driver needed based on your connection string!

# Import ipython-sql Jupyter extension to create SQL cells
%load_ext sql
%sql duckdb:///:memory:

```


```sql
%%sql
SELECT
    utctime,
    ask, bid,
    (ask + bid)/2.0 as mid,
    utctime + INTERVAL 91 second as WindowEnd,
    last(bid) OVER(ORDER BY utctime range BETWEEN INTERVAL 0 second preceding AND INTERVAL 91 second following) as Windowbid,
    last(ask) OVER(ORDER BY utctime range BETWEEN INTERVAL 0 second preceding AND INTERVAL 91 second following) as Windowask
FROM US500Ticks
ORDER BY utctime 
LIMIT 5
```

     * duckdb:///:memory:
    Done.





<table>
    <tr>
        <th>utctime</th>
        <th>ask</th>
        <th>bid</th>
        <th>mid</th>
        <th>WindowEnd</th>
        <th>Windowbid</th>
        <th>Windowask</th>
    </tr>
    <tr>
        <td>2021-01-03 23:00:10.072000</td>
        <td>3758.100000</td>
        <td>3757.500000</td>
        <td>3757.8</td>
        <td>2021-01-03 23:01:41.072000</td>
        <td>3758.300000</td>
        <td>3758.900000</td>
    </tr>
    <tr>
        <td>2021-01-03 23:00:10.486000</td>
        <td>3758.500000</td>
        <td>3757.900000</td>
        <td>3758.2</td>
        <td>2021-01-03 23:01:41.486000</td>
        <td>3757.900000</td>
        <td>3758.500000</td>
    </tr>
    <tr>
        <td>2021-01-03 23:00:10.885000</td>
        <td>3758.600000</td>
        <td>3758.000000</td>
        <td>3758.3</td>
        <td>2021-01-03 23:01:41.885000</td>
        <td>3757.900000</td>
        <td>3758.500000</td>
    </tr>
    <tr>
        <td>2021-01-03 23:00:11.091000</td>
        <td>3758.900000</td>
        <td>3758.300000</td>
        <td>3758.6</td>
        <td>2021-01-03 23:01:42.091000</td>
        <td>3757.900000</td>
        <td>3758.500000</td>
    </tr>
    <tr>
        <td>2021-01-03 23:00:11.686000</td>
        <td>3759.000000</td>
        <td>3758.400000</td>
        <td>3758.7</td>
        <td>2021-01-03 23:01:42.686000</td>
        <td>3757.600000</td>
        <td>3758.200000</td>
    </tr>
    <tr>
        <td>2021-01-03 23:00:12.113000</td>
        <td>3759.100000</td>
        <td>3758.500000</td>
        <td>3758.8</td>
        <td>2021-01-03 23:01:43.113000</td>
        <td>3757.400000</td>
        <td>3758.000000</td>
    </tr>
    <tr>
        <td>2021-01-03 23:00:12.487000</td>
        <td>3759.400000</td>
        <td>3758.800000</td>
        <td>3759.1</td>
        <td>2021-01-03 23:01:43.487000</td>
        <td>3757.400000</td>
        <td>3758.000000</td>
    </tr>
    <tr>
        <td>2021-01-03 23:00:13.685000</td>
        <td>3759.200000</td>
        <td>3758.600000</td>
        <td>3758.9</td>
        <td>2021-01-03 23:01:44.685000</td>
        <td>3757.400000</td>
        <td>3758.000000</td>
    </tr>
    <tr>
        <td>2021-01-03 23:00:13.887000</td>
        <td>3760.100000</td>
        <td>3759.500000</td>
        <td>3759.8</td>
        <td>2021-01-03 23:01:44.887000</td>
        <td>3757.400000</td>
        <td>3758.000000</td>
    </tr>
    <tr>
        <td>2021-01-03 23:00:14.884000</td>
        <td>3759.600000</td>
        <td>3759.000000</td>
        <td>3759.3</td>
        <td>2021-01-03 23:01:45.884000</td>
        <td>3757.000000</td>
        <td>3757.600000</td>
    </tr>
</table>




```python
%%time
_cacheFiles = "/mnt/z/Users/Home/AppData/Roaming/XXXXXXXXXXX/BacktestingCache/XXXXXXXXX/NZDUSD/Ticks"
NZDUSDTicks = cache.GetBackTestData(_cacheFiles,"2021.01.01","2021.03.01")
```

    CPU times: user 5.85 s, sys: 177 ms, total: 6.03 s
    Wall time: 7.21 s



```sql
%%sql
SELECT
    utctime,
    ask, bid,
    (ask + bid)/2.0 as mid,
    utctime + INTERVAL 91 second as WindowEnd,
    last(bid) OVER(ORDER BY utctime range BETWEEN INTERVAL 0 second preceding AND INTERVAL 91 second following) as Windowbid,
    last(ask) OVER(ORDER BY utctime range BETWEEN INTERVAL 0 second preceding AND INTERVAL 91 second following) as Windowask
FROM NZDUSDTicks
ORDER BY utctime 
LIMIT 10
```

     * duckdb:///:memory:
    Done.





<table>
    <tr>
        <th>utctime</th>
        <th>ask</th>
        <th>bid</th>
        <th>mid</th>
        <th>WindowEnd</th>
        <th>Windowbid</th>
        <th>Windowask</th>
    </tr>
    <tr>
        <td>2021-01-03 22:01:00.004000</td>
        <td>0.719410</td>
        <td>0.719080</td>
        <td>0.719245</td>
        <td>2021-01-03 22:02:31.004000</td>
        <td>0.719010</td>
        <td>None</td>
    </tr>
    <tr>
        <td>2021-01-03 22:01:01.105000</td>
        <td>None</td>
        <td>0.719100</td>
        <td>None</td>
        <td>2021-01-03 22:02:32.105000</td>
        <td>0.719010</td>
        <td>None</td>
    </tr>
    <tr>
        <td>2021-01-03 22:01:01.909000</td>
        <td>None</td>
        <td>0.719080</td>
        <td>None</td>
        <td>2021-01-03 22:02:32.909000</td>
        <td>0.719010</td>
        <td>None</td>
    </tr>
    <tr>
        <td>2021-01-03 22:01:02.429000</td>
        <td>None</td>
        <td>0.719070</td>
        <td>None</td>
        <td>2021-01-03 22:02:33.429000</td>
        <td>0.719010</td>
        <td>None</td>
    </tr>
    <tr>
        <td>2021-01-03 22:01:02.954000</td>
        <td>None</td>
        <td>0.719050</td>
        <td>None</td>
        <td>2021-01-03 22:02:33.954000</td>
        <td>0.719010</td>
        <td>None</td>
    </tr>
    <tr>
        <td>2021-01-03 22:01:03.307000</td>
        <td>None</td>
        <td>0.719030</td>
        <td>None</td>
        <td>2021-01-03 22:02:34.307000</td>
        <td>0.719010</td>
        <td>None</td>
    </tr>
    <tr>
        <td>2021-01-03 22:01:03.548000</td>
        <td>None</td>
        <td>0.719010</td>
        <td>None</td>
        <td>2021-01-03 22:02:34.548000</td>
        <td>0.719010</td>
        <td>None</td>
    </tr>
    <tr>
        <td>2021-01-03 22:03:02.290000</td>
        <td>0.719350</td>
        <td>0.719060</td>
        <td>0.719205</td>
        <td>2021-01-03 22:04:33.290000</td>
        <td>0.718970</td>
        <td>None</td>
    </tr>
    <tr>
        <td>2021-01-03 22:03:18.453000</td>
        <td>0.719410</td>
        <td>0.719010</td>
        <td>0.71921</td>
        <td>2021-01-03 22:04:49.453000</td>
        <td>0.718990</td>
        <td>None</td>
    </tr>
    <tr>
        <td>2021-01-03 22:03:31.321000</td>
        <td>0.719240</td>
        <td>None</td>
        <td>None</td>
        <td>2021-01-03 22:05:02.321000</td>
        <td>0.718970</td>
        <td>None</td>
    </tr>
</table>
