#mapping adapted from http://tinyurl.com/qe8554s
import time, csv
from googlefinance import getQuotes
from elasticsearch import Elasticsearch
from time import strftime

'''
# Map the fields of a new "stock" doc_type
mapping = {
    "stock": {
        "properties": {
            "Open": {"type": "float"},
            "High": {"type": "float"},
            "Low": {"type": "float"},
            "Close": {"type": "float"},
            "Volume": {"type": "float"},
            "StockSymbol": {"type": "string"},
            "tm_year": {"type": "integer"},
            "tm_month": {"type": "integer"},
            "tm_mday": {"type": "integer"},
            "tm_hour": {"type": "integer"},
            "tm_min": {"type": "integer"},
            "tm_sec": {"type": "integer"},
            "tm_wday": {"type": "integer"},
            "tm_yday": {"type": "integer"},
            #YYYY-MM-DD HH:mm:ss.SSS
            "LastTradeDateTime": {"type": "date", "format": "basic_date_time_no_millis"}, 
            #"LastTradeDateTime": {"type": "date", "format": "basic_date_time_no_millis"},
            "ID": {"type": "float"}
        }
    }
}
'''

# Map the fields of a new "stock" doc_type
mapping = {
    "stock": {
        "properties": {
            "open": {"type": "float"},
            "high": {"type": "float"},
            "low": {"type": "float"},
            "close": {"type": "float"},
            "volume": {"type": "float"},
            "symbol": {"type": "string"},
            "symbol-id": {"type": "integer"},
            "t-year": {"type": "integer"},
            "t-month": {"type": "integer"},
            "t-mday": {"type": "integer"},
            "t-wday": {"type": "integer"},
            "t-yday": {"type": "integer"},
            #YYYY-MM-DD HH:mm:ss.SSS
            "t-date": {"type": "date"}
            #"LastTradeDateTime": {"type": "date", "format": "basic_date_time_no_millis"},
        }
    }
}

# Create a new "moneyverse" index that includes "stock" with the above mapping
es = Elasticsearch([{'host': '159.203.74.153', 'port': 9200}])
es.indices.create("moneyverse", ignore=400)
es.indices.put_mapping(index="moneyverse", doc_type="stock", body=mapping)

# Import a CSV file of trip data - this will take quite a while!

with open('table_aapl.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile)
    #reader.next() # Skip header row
    for id, row in enumerate(reader):
        tm=time.strptime(row[0], "%Y%m%d")
        content = {
            "open": float(row[2]),
            "high": float(row[3]),
            "low": float(row[4]),
            "close": float(row[5]),
            "volume": float(row[6]),
            "symbol": "aapl",
            "symbol-id": 1,
            "t-year": tm[0],
            "t-month": tm[1],
            "t-mday": tm[2],
            "t-wday": tm[6]+1,
            "t-yday": tm[7],
            "t-date": strftime("%Y-%m-%dT11:30:00", tm), 
                #improve: speed up using native ELK date handlers? 
                #improve: timezone management?
        }
        #print content
        #time.sleep(1)
        es.index(index="moneyverse", doc_type='stock', id=id, body=content)
        reader.next() # Skip header row