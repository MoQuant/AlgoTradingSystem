# Holds key and secret
def auth():
    key = ''
    secret = ''
    return key, secret

import urllib.parse
import hashlib
import hmac
import base64
import time 
import json
import requests
import websocket
import threading
import numpy as np 

# Timestamp for authenticated functions
stamp = lambda: str(int(time.time()*1000))

# Timestamp for ping command
T = lambda: int(time.time())

# Ping message sent to server 
ping = lambda: json.dumps({'event':'ping','reqid':47})

class Math:

    # Stores prices from feed by ticker
    store_prices = {}

    # Extracts and stores price from socket message
    def ExtractData(self, resp):
        ticker = resp[-1]
        price = float(resp[1][0][0])
        if ticker not in self.store_prices.keys():
            self.store_prices[ticker] = []
        self.store_prices[ticker].append(price)

    # Calculates moving averages for each pair
    def MovingAverage(self, periods=12):
        moving_average = {}
        for ticker in self.store_prices:
            if len(self.store_prices[ticker]) >= 2:
                moving_average[ticker] = np.mean(self.store_prices[ticker][-periods:])
        return moving_average


# Inherits Math class so computations can run
class Kraken(Math):

    def __init__(self, tickers=['XBT/USD','ETH/USD']):
        self.key, self.secret = auth()
        self.tickers = tickers

        self.rest_url = 'https://api.kraken.com'
        self.ws_url = 'wss://ws.kraken.com'

        # HTTPS session for faster connection
        self.session = requests.Session()

    # SHA512/256 & Base64 authenticated function encryption
    def signature(self, urlpath, data):
        postdata = urllib.parse.urlencode(data)
        encoded = (str(data['nonce']) + postdata).encode()
        message = urlpath.encode() + hashlib.sha256(encoded).digest()
        mac = hmac.new(base64.b64decode(self.secret), message, hashlib.sha512)
        sigdigest = base64.b64encode(mac.digest())
        return sigdigest.decode()

    # Places trades and fetches balance
    def communicate(self, uri_path, data):
        headers = {}
        headers['API-Key'] = self.key 
        headers['API-Sign'] = self.signature(uri_path, data)             
        req = requests.post((self.rest_url + uri_path), headers=headers, data=data)
        return req.json()

    # Fetches balance from Kraken
    def Balance(self):
        endpoint = '/0/private/Balance'
        msg = {
            'nonce': stamp()
        }
        return self.communicate(endpoint, msg)

    # Places a market buy order
    def MarketBuy(self, pair, volume):
        endpoint = '/0/private/AddOrder'
        msg = {
            'nonce': stamp(),
            'ordertype': 'market',
            'type': 'buy',
            'volume': volume,
            'pair': pair
        }    
        return self.communicate(endpoint, msg)

    # Places a market sell order
    def MarketSell(self, pair, volume):
        endpoint = '/0/private/AddOrder'
        msg = {
            'nonce': stamp(),
            'ordertype': 'market',
            'type': 'sell',
            'volume': volume,
            'pair': pair
        }    
        return self.communicate(endpoint, msg)

    # WebSocket data feed
    def DataFeed(self):
        self.connection = websocket.create_connection(self.ws_url)
        server_message = {
            'event':'subscribe',
            'pair':self.tickers,
            'subscription':{
                'name':'trade'
            }
        }
        self.connection.send(json.dumps(server_message))

        t0 = T()
        while True:
            resp = self.connection.recv()
            resp = json.loads(resp)
            if type(resp) == list:
                self.ExtractData(resp)
            

            if T() - t0 > 60:
                self.connection.send(ping())
                t0 = T()


kraken = Kraken()

datafeed = threading.Thread(target=kraken.DataFeed)
datafeed.start()

trade = {tick:'neutral' for tick in kraken.tickers}

# Trading strategy
while True:
    ma_1 = kraken.MovingAverage(periods=7)
    ma_2 = kraken.MovingAverage(periods=14)
    
    for ticker in kraken.tickers:
        if ticker in ma_1.keys() and ticker in ma_2.keys():
            lower_ma = ma_1[ticker]
            higher_ma = ma_2[ticker]
            last_price = kraken.store_prices[ticker][-1]
            print(ticker, last_price, len(kraken.store_prices[ticker]), lower_ma, higher_ma)
            if lower_ma < higher_ma and trade[ticker] == 'neutral':
                print("Buy: ", ticker, " at ", last_price)
                trades = kraken.MarketBuy(ticker.replace('/',''), 1)
                print(trades)
                trade[ticker] = 'long'
            
            if lower_ma > higher_ma and trade[ticker] == 'long':
                print("Sell: ", ticker, " at ", last_price)
                trades = kraken.MarketSell(ticker.replace('/',''), 1)
                print(trades)
                trade[ticker] = 'neutral'

    
    
    time.sleep(0.25)

datafeed.join()
