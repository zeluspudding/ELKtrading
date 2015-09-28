import socket, json, time
from googlefinance import getQuotes
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': '159.203.74.153', 'port': 9200}])

if __name__ == '__main__':
    try:
        i = 1 #Prepare to iterate over samples
        while True:
            payload = getQuotes('AAPL')[0]
            #data = {'message': 'temperature %.1f cm' % temperature, 'temperature': temperature, 'hostname': socket.gethostname()}
            print "Last Trade Price:", float(payload['LastTradePrice'])
            es.index(index='stocks1', doc_type='stimuli', id=i, body=json.dumps(payload))
            i=i+1
            time.sleep(5)
# interrupt
    except KeyboardInterrupt:
        print("Programm interrupted")
