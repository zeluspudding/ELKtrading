import socket, json, time
from googlefinance import getQuotes
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': '159.203.74.153', 'port': 9200}])

if __name__ == '__main__':
    try:
        i = 1 #Prepare to iterate over samples
        while True:
            data = getQuotes('TSLA')[0]
            payload={
                "Index": "NASDAQ",
                "LastTradeWithCurrency": float(data['LastTradeWithCurrency']),
                "LastTradeDateTime": "2015-09-29T15:25:58Z",
                "LastTradePrice": float(data['LastTradePrice']),
                "StockSymbol": "AAPL",
                "ID": float(data['ID'])
                }
            print "Last Trade Price:", float(data['LastTradePrice'])
            es.index(index='stocks1', doc_type='stimuli', id=i, body=payload)
            #es.index(index='stocks1', doc_type='stimuli', id=i, body=json.dumps(payload))
            i=i+1
            time.sleep(5)
# interrupt
    except KeyboardInterrupt:
        print("Programm interrupted")
