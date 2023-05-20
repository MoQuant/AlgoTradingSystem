# Import the necessary libraries
import asyncio
import aiohttp
import json
import numpy as np

from kraken import Auth

# Import key and secret
key, secret = [l.replace('\n','') for l in open('./auth.txt', 'r').readlines()]

# Strategy class
class Strats:

    positions = {}

    async def StrategyOne(self, session):
        # Import tickers and rsi measures
        tickers = self.tickers
        rsi_tech = self.RSI()

        # Loop through tickers
        for tick in tickers:
            # Pull selected ticker data
            rsi = rsi_tech[tick]
            prices = self.store_data[tick]
            p, v = np.array(prices).T
            current_price = p[-1]

            # Check if ticker has loaded
            if tick not in self.positions.keys():
                self.positions[tick] = 'neutral'

            # Place algorithmic trades
            if tick  in self.positions.keys():
                if rsi > 65 and self.positions[tick] == 'neutral':
                    # BUY ORDER
                    print(f'Buy: {tick} at price: {current_price}')
                    self.positions[tick] = 'long'
                    resp = await self.api.limit_buy(session, current_price, 1, tick)
                    print(resp)

                if rsi < 30 and self.positions[tick] == 'long':
                    # SELL ORDER
                    print(f'Sell: {tick} at price: {current_price}')
                    self.positions[tick] = 'neutral'
                    resp = await self.api.limit_sell(session, current_price, 1, tick)
                    print(resp)
            


# Database class
class Data(Strats):

    # Stores the price data and sets a limit on storage
    store_data = {}
    limit = 50

    # Parses the price tick data
    def parse(self, resp):
        # Only lets variable in when it is a list
        if type(resp) == list:
            ticker = resp[-1]            # Extracts ticker
            body = resp[1]               # Extracts prices
            price = float(body['c'][0])  # Price
            volume = float(body['c'][1]) # Volume

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
            up = np.sum([a - b for a, b in zip(A, B) if a - b >= 0])/len(A)
            down = np.sum([abs(a - b) for a, b in zip(A, B) if a - b < 0])/len(B)
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
                await self.StrategyOne(session)
                print(self.store_data, self.RSI())

                await asyncio.sleep(0.001)



Kraken()