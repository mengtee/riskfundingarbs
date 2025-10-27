import os 
import asyncio
from dotenv import load_dotenv
import logging
import collections
from pprint import pprint
import numpy as np
from quantpylib.gateway.master import Gateway

load_dotenv()

def get_key():
    config_keys = {
        'binance':{
            'key': os.getenv('BINANCE_API_KEY'),
            'secret': os.getenv('BINANCE_API_SECRET')
        },
        'hyperliquid':{
            'key': os.getenv('HYPERLIQUID_API_KEY'),
            'secret': os.getenv('HYPERLIQUID_API_SECRET')
        }
    } 
    return config_keys

class MarketData():
    def __init__(self, gateway, exchanges, preference_quote = ['USDT', 'USDC']):
        self.gateway = gateway
        self.exchanges = exchanges
        self.universe = {}
        self.preference_quote = preference_quote
        
        self.overlapped_assets = {}
        self.base_mappings = {exc: {} for exc in exchanges}
        
    async def search_overlap_asset(self, gateway, exchanges):   
        assets = {}
        
        for exchange in exchanges:
            tickers = await gateway.exchange.clients[exchange].get_all_mids()
            assets[exchange] = tickers
        
        binance_assets = {s[:-4] if s.endswith(('USDC', 'USDT')) else s for s in assets['binance']}
        hyperliquid_assets = set(assets['hyperliquid'].keys())
        self.overlapped_assets = binance_assets & hyperliquid_assets

        return self.overlapped_assets 
    
    async def serve_exchanges(self):
        return await asyncio.gather(*[
            self.serve(exc) for exc in self.exchanges
        ])
        
    async def serve(self, exchange):
        return await asyncio.gather(*[
            self.serve_base_mappings(self.gateway, exchange),
            self.serve_l2_stream(self.gateway, exchange),
            self.serve_balance(self.gateway, exchange)
        ])
        
    async def serve_base_mappings(self, gateway, exchange):
        while True:
            try:
                perps_data = await gateway.exchange.contract_specifications(exc=exchange)
                funding_data = await gateway.exchange.get_funding_info(exc=exchange)
                pprint(perps_data)
                mappings = collections.defaultdict(list)
                assets = self.overlapped_assets
                
                for k, v in perps_data.items():
                    if v['base_asset'] in assets and v['quote_asset'] in ['USDT', "USDC", "USD"]:
                        if 'PERP' in k or not any(x in k for x in ['-C', '-P']):
                            funding_info = funding_data.get(k, {})
                            combined_data = {**v, **funding_info, 'symbol':k}
                            mappings[v['base_asset']].append(combined_data)
                remove = set()
                for k,v in mappings.items():
                    if len(v) > 1 and self.preference_quote is not None:
                        mappings[k] = [x for x in v if x['quote_asset'] in self.preference_quote]
                    if len(mappings[k]) ==0:
                        remove.add(k)
                for k in remove:
                    remove.pop(k)
                    
                if exchange == "binance":
                    for k, v in mappings.items():
                        for contract in v:
                            contract['frint'] = np.float64(1)
                
                mappings = dict(sorted(mappings.items()))
                
                self.base_mappings[exchange] = mappings
                self.universe[exchange] = mappings
                await asyncio.sleep(60*4)
                
            except Exception as e:
                print(f'Unknown error {e}')
                
    async def serve_l2_stream(self, gateway, exchange):
        universe = self.universe
        while not universe or exchange not in universe:
            await asyncio.sleep(15)
            
        assets = self.overlapped_assets
        base_mappings = universe[exchange]
        
        tickers = []
        for asset in assets:
            if asset in base_mappings:
                tickers.extend([ticker['symbol'] for ticker in base_mappings[asset]])
        
        logging.info(f'[{exchange}] L2 Ticker Streams: {tickers}')
        if exchange == 'binance': tickers.append('USDCUSDT')
        
        lobs = await asyncio.gather(*[
            gateway.executor.l2_book_mirror(
                ticker=ticker,
                speed_ms=500,
                exc=exchange,
                depth=20,
                as_dict=False,
                buffer_size=10,
            )for ticker in tickers
        ])
        
        for ticker, lob in zip(tickers, lobs):
            self.l2_ticker_streams[exchange][ticker] = lob
        logging.info(f'Updated L2 Streams for {exchange} and tickers: {list(self.l2_ticker_streams[exchange].keys())}')
        
        await asyncio.sleep(1e9)
    
    async def serve_balance(self, gateway, exchange, poll_interval=100):
        while True:
            try:
                bal = await gateway.account.account_balance(exc=exchange)
                self.balances[exchange] = bal
                await asyncio.sleep(poll_interval)
            except Exception as e:
                logging.error(f'[{exchange}] Error in fetching balances: {e}')
                await asyncio.sleep(15)

async def main():
    gateway = Gateway(config_keys=get_key())
    await gateway.init_clients()
    
    market_data = MarketData(
        gateway=gateway,
        exchanges=['binance', 'hyperliquid'],
        preference_quote='USDT'
    )
    
    asyncio.create_task(market_data.serve_exchanges())
    await asyncio.sleep(30*90)
    await gateway.cleanup_clients()
    
if __name__ == "__main__":
    asyncio.run(main())
        