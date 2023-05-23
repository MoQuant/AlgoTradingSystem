# Import the necessary libraries
import asyncio
import aiohttp
import json
import numpy as np
import time

from kraken import Auth

# Imports current timestamp
stamp = lambda: int(time.time())

# Import key and secret
key, secret = [l.replace('\n','') for l in open('./auth.txt', 'r').readlines()]

# Strategy class
class Strategy:

    positions = {}
    pos_times = {}

    async def wait_for_fill(self, session, resp, positionA, positionB, volume, tick, stop=6):
        transaction_id = resp['result']['txid']
        for look in range(stop):
            orders = await self.api.open_orders(session)
            cleared_volume = orders[transaction_id]['vol_exec']
            vol = volume - cleared_volume
            if vol <= 0.00001:
                break
            await asyncio.sleep(0.5)

        if vol > 0.00001:
            if positionA == 'neutral':
                if positionB == 'long':

                    resp = await self.api.market_buy(session, vol, tick)
                    print("Going long: ", resp)
                    return 'long'

                if positionB == 'short':

                    resp = await self.api.market_sell(session, vol, tick)
                    print("Going short: ", resp)
                    return 'short'

            if positionA == 'long':

                if positionB == 'neutral':
                    resp = await self.api.market_sell(session, vol, tick)
                    print('Close long: ', resp)
                    return 'neutral'

            if positionA == 'short':
                if positionB == 'neutral':
                    resp = await self.api.market_buy(session, vol, tick)
                    print('Close short: ', resp)
                    return 'neutral'
                


        if positionA == 'neutral' and positionB == 'long':
            return 'long'
        if positionA == 'neutral' and positionB == 'short':
            return 'short'
        if positionA == 'long' and positionB == 'neutral':
            return 'neutral'
        if positionA == 'short' and positionB == 'neutral':
            return 'neutral'


    async def StrategyOne(self, session, volume=1.0, trade_time=20):

        # Loop through all tickers
        for tick in self.tickers:
            # Extract RSI
            RSI = self.RSI()

            # Extract correct tick
            tick2 = self.assets[tick]

            if tick in RSI.keys():
                rsi = RSI[tick]

                # Load bid/ask
                bid = self.highest_bid[tick]
                ask = self.lowest_ask[tick]

                # Check if ticker is in positions
                if tick not in self.positions.keys():
                    self.positions[tick] = 'neutral'

                # Short Trade
                if self.positions[tick] == 'neutral' and rsi < 35:
                    print(f'Short Order {tick} at {bid}')
                    resp = await self.api.limit_sell(session, bid, volume, tick2)
                    self.positions[tick] = await self.wait_for_fill(session, resp, 'neutral', 'short', volume, tick2, stop=6)
                    self.pos_times[tick] = stamp()
                
                # Close Short
                if self.positions[tick] == 'short' and rsi > 60:
                    print(f'Close Short {tick} at {ask}')
                    resp = await self.api.limit_buy(session, ask, volume, tick2)
                    self.positions[tick] = await self.wait_for_fill(session, resp, 'short', 'neutral', volume, tick2, stop=6)
                    
                # Close short if position exceeds limit
                if self.positions[tick] == 'short' and stamp() - self.pos_times[tick] > trade_time:
                    print(f'Close Short {tick} at {ask}')
                    resp = await self.api.limit_buy(session, ask, volume, tick2)
                    self.positions[tick] = await self.wait_for_fill(session, resp, 'short', 'neutral', volume, tick2, stop=6)
                    


                # Long Trade
                if self.positions[tick] == 'neutral' and rsi > 65:
                    print(f'Long Order {tick} at {ask}')
                    resp = await self.api.limit_buy(session, ask, volume, tick2)
                    self.positions[tick] = await self.wait_for_fill(session, resp, 'neutral', 'long', volume, tick2, stop=6)
                    self.pos_times[tick] = stamp()
                
                # Close Long
                if self.positions[tick] == 'long' and rsi < 35:
                    print(f'Close Long {tick} at {bid}')
                    resp = await self.api.limit_sell(session, bid, volume, tick2)
                    self.positions[tick] = await self.wait_for_fill(session, resp, 'long', 'neutral', volume, tick2, stop=6)
                    

                # Close long if position exceeds limit
                if self.positions[tick] == 'long' and stamp() - self.pos_times[tick] > trade_time:
                    print(f'Close Long {tick} at {bid}')
                    resp = await self.api.limit_sell(session, bid, volume, tick2)
                    self.positions[tick] = await self.wait_for_fill(session, resp, 'long', 'neutral', volume, tick2, stop=6)
                    
            



# Database class
class Data(Strategy):

    # Stores the price data and sets a limit on storage
    store_data = {}
    limit = 50

    highest_bid = {}
    lowest_ask = {}

    # Parses the price tick data
    def parse(self, resp):
        # Only lets variable in when it is a list
        if type(resp) == list:
            ticker = resp[-1]            # Extracts ticker
            body = resp[1]               # Extracts prices
            price = float(body['c'][0])  # Price
            volume = float(body['c'][1]) # Volume

            self.highest_bid[ticker] = float(body['b'][0])
            self.lowest_ask[ticker] = float(body['a'][0])

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
            
            # List of assets
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
            await rss.send_str(json.dumps(msg2))

            while True:
                resp = await rss.receive()
                resp = json.loads(resp.data)

                # STRATEGY GOES HERE
                await self.StrategyOne(session)

                await asyncio.sleep(0.001)



Kraken()