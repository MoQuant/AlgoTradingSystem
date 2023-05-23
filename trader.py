# Import the necessary libraries
import asyncio
import aiohttp
import json
import numpy as np
import time 

from kraken import Auth

# Import key and secret
key, secret = [l.replace('\n','') for l in open('./auth.txt', 'r').readlines()]

class Strats:

    position = {}

    async def execute_trade(self, session, conn, resp, position, volume, stop=7):

        transaction_id = resp['result']['txid'] # Fetch transaction ID

        t0 = int(time.time()) # Intialize beginning trade timestamp

        for epoch in range(stop):
            sock = await conn.receive()
            sock = json.loads(sock)
            if 'channelName' in sock.keys():
                if sock['channelName'] == 'openOrders':
                    order = float(sock[transaction_id]['vol_exec'])
                    vol = volume - order
                    if vol <= 0.00001:
                        break
                    await asyncio.sleep(0.5)

        if vol >= 0.01:
            for i in self.tickers:
                if position[i] == 'neutral':
                    resp = await self.api.market_buy(session, vol, i)
                    return 'long'
                if position[i] == 'long':
                    resp = await self.api.market_sell(session, vol, i)
                    return 'neutral'
                

        return 'long' if position == 'neutral' else 'neutral'




    async def StrategyOne(self, session, conn):
        # Define base volume
        vol = 0.5

        # Fetches the RSI for all tickers
        RSI = self.RSI()

        for i in self.tickers:
            # Selects RSI
            rsi = RSI[i]

            # Checks if ticker is not in position dictionary
            if i not in self.position.keys():
                self.position[i] = 'neutral'

            if self.position[i] == 'long' and rsi < 33:
                print("Execute a sell order")
                resp = await self.api.limit_sell(session, self.highest_bid, vol, i)

                self.positon[i] = await self.execute_trade(session, conn, vol, position[i], resp)

            if self.position[i] == 'neutral' and rsi > 65:
                print("Execute a buy order")
                resp = await self.api.limit_buy(session, self.lowest_ask, vol, i)

                # Check to see if order has executed
                self.position[i] = await self.execute_trade(session, conn, vol, position[i], resp)
                
            


# Database class
class Data(Strats):

    # Stores the price data and sets a limit on storage
    store_data = {}
    limit = 50

    highest_bid = 0
    lowest_ask = 0

    # Parses the price tick data
    def parse(self, resp):
        # Only lets variable in when it is a list
        if type(resp) == list:
            ticker = resp[-1]            # Extracts ticker
            body = resp[1]               # Extracts prices
            price = float(body['c'][0])  # Price
            volume = float(body['c'][1]) # Volume

            # Fetches highest bid
            self.highest_bid = float(body['b'][0])

            # Fetches lowest ask
            self.lowest_ask = float(body['a'][0])

            # Checks to see if ticker is not in dataset
            if ticker not in self.store_data.keys():
                self.store_data[ticker] = []

            # Appends price and volume to data storage
            if ticker in self.store_data.keys():
                self.store_data[ticker].append([price, volume])
        
        # Deletes excess rows
        for i in self.store_data:
            if len(self.store_data[i]) >= self.limit:
                del self.store_data[i][0]

    # RSI formula
    def rsi(self, up, down):
        return 100 - 100 / (1 + up/down)

    # Calculates RSI
    def RSI(self):
        result = {}
        for ticker, x in self.store_data.items():
            p, v = np.array(x).T
            A, B = p[1:], p[:-1]
            up = np.sum([a - b for a, b in zip(A, B) if a - b >= 0])/len(p)
            down = np.sum([abs(a - b) for a, b in zip(A, B) if a - b < 0])/len(p)
            result[ticker] = self.rsi(up, down)
        return result


# Declare class Kraken to hold all items of this trading system
class Kraken(Data):

    def __init__(self, tickers=['XBT/USD','ETH/USD'], tick_limit=10):
        # Trading functions
        self.api = Auth(key, secret)

        # URLs
        self.public_ws_url = 'wss://ws.kraken.com'
        self.private_ws_url = 'wss://ws-auth.kraken.com'

        # Set tick limit
        self.tick_limit = tick_limit 

        # Store tickers
        self.tickers = tickers 
        print("System Booted..........")

        # Declares and runs the loop
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.system())

    # Core of this program
    async def system(self):
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            
            # Fetches all asset pairs to match socket format to trading format
            self.assets = await self.api.asset_pairs(session)
            
            # Creates two parallel sockets
            tasks = [asyncio.ensure_future(self.private_socket(session)),
                     asyncio.ensure_future(self.public_socket(session))]

            # Runs the sockets
            await asyncio.wait(tasks)
            
            
    # Socket which fetches crypto data
    async def public_socket(self, session):
        
        async with session.ws_connect(self.public_ws_url) as wss:
            msg = {'event':'subscribe','pair':self.tickers,'subscription':{'name':'ticker'}}
            await wss.send_str(json.dumps(msg))
            while True:
                resp = await wss.receive()
                resp = json.loads(resp.data)
                self.parse(resp)
                await asyncio.sleep(0.001)
        

    # Socket which fetches account data and trades
    async def private_socket(self, session):

        # Generates you an authenticated token
        tokenz = await self.api.token(session)

        # Subscribes to ownTrades
        msg = {"event": "subscribe",
               "subscription":
                    {
                        "name": "ownTrades",
                        "token": tokenz
                    }
                }

        # Subscribes to openOrders
        msg2 = {"event": "subscribe",
               "subscription":
                    {
                        "name": "openOrders",
                        "token": tokenz
                    }
                }
        async with session.ws_connect(self.private_ws_url) as rss:

            # Send kraken server creds
            await rss.send_str(json.dumps(msg))
            await rss.send_str(json.dumps(msg2))

            while True:
                resp = await rss.receive()
                resp = json.loads(resp.data)

                # STRATEGY GOES HERE
                await self.StrategyOne(session, rss)

                await asyncio.sleep(0.001)



Kraken()