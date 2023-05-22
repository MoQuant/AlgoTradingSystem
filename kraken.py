import urllib.parse
import hashlib
import hmac
import base64
import time 
import json

# Timestamp
dt = lambda: int(time.time()*1000)

class Auth:

    def __init__(self, key, secret):
        self.key = key 
        self.secret = secret
        self.rest_url = 'https://api.kraken.com'

    # Exchange signature process
    def sign(self, urlpath, data, secret):

        postdata = urllib.parse.urlencode(data)
        encoded = (str(data['nonce']) + postdata).encode()
        message = urlpath.encode() + hashlib.sha256(encoded).digest()

        mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
        sigdigest = base64.b64encode(mac.digest())
        return sigdigest.decode()

    # Exchange request function
    async def request(self, session, uri_path, data):
        headers = {}
        headers['API-Key'] = self.key
        
        headers['API-Sign'] = self.sign(uri_path, data, self.secret)            
        #req = requests.post((api_url + uri_path), headers=headers, data=data)
        async with session.post(self.rest_url + uri_path, headers=headers, data=data) as response:
            result = await response.text()
            return json.loads(result)

    # Fetch balance
    async def balance(self, session):
        endpoint = '/0/private/Balance'
        data = {'nonce': dt()}
        resp = await self.request(session, endpoint, data)
        return resp

    # Fetch trade balance
    async def trade_balance(self, session, asset='USD'):
        endpoint = '/0/private/TradeBalance'
        data = {'nonce':dt(), 'asset':asset}
        resp = await self.request(session, endpoint, data)
        return resp

    # Places a limit buy order
    async def limit_buy(self, session, price, volume, pair):
        endpoint = '/0/private/AddOrder'
        data = {'nonce':dt(), 'ordertype': 'limit', 'type': 'buy','price':price, 'volume':volume, 'pair':pair}
        resp = await self.request(session, endpoint, data)
        return resp

    # Places a limit sell order
    async def limit_sell(self, session, price, volume, pair):
        endpoint = '/0/private/AddOrder'
        data = {'nonce':dt(), 'ordertype': 'limit', 'type': 'sell','price':price, 'volume':volume, 'pair':pair}
        resp = await self.request(session, endpoint, data)
        return resp

    # Places a market buy order
    async def market_buy(self, session, volume, pair):
        endpoint = '/0/private/AddOrder'
        data = {'nonce':dt(), 'ordertype': 'market', 'type': 'buy','volume':volume, 'pair':pair}
        resp = await self.request(session, endpoint, data)
        return resp

    # Places a market sell order
    async def market_sell(self, session, volume, pair):
        endpoint = '/0/private/AddOrder'
        data = {'nonce':dt(), 'ordertype': 'market', 'type': 'sell','volume':volume, 'pair':pair}
        resp = await self.request(session, endpoint, data)
        return resp

    # Cancels an order
    async def cancel_order(self, session, txid):
        endpoint = '/0/private/CancelOrder'
        data = {'nonce':dt(), 'txid': txid}
        resp = await self.request(session, endpoint, data)
        return resp

    # Returns closed orders
    async def closed_order(self, session):
        endpoint = '/0/private/ClosedOrders'
        data = {'nonce': dt()}
        resp = await self.request(session, endpoint, data)
        return resp

    # Generate authentication token
    async def token(self, session):
        endpoint = '/0/private/GetWebSocketsToken'
        data = {'nonce':dt()}
        resp = await self.request(session, endpoint, data)
        return resp['result']['token']

    async def AssetPairs(self, session):
        endpoint = '/0/public/AssetPairs'
        data = {'nonce':dt()}
        resp = await self.request(session, endpoint, data)
        result = {}
        for i, j in resp['result'].items():
            result[j['wsname']] = j['altname']
        return result

