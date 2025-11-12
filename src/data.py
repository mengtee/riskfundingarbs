import os 
import asyncio
from dotenv import load_dotenv
import logging
import collections
from pprint import pprint
import numpy as np
from quantpylib.gateway.master import Gateway

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')

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
        self.l2_ticker_streams = {exc:{} for exc in exchanges}
        self.balances = {exc:None for exc in exchanges}
        self.ready_flags = {exc: {'mappings': False, 'streams': False} for exc in exchanges}
        self.ready_event = asyncio.Event()
    
    def is_ready(self):
        for exc in self.exchanges:
            flags = self.ready_flags[exc]
            if not (flags['mappings'] and flags['streams']):
                return False
        return True
    
    async def wait_until_ready(self):
        while not self.is_ready():
            await asyncio.sleep(10)
            logging.info('Waiting for data streams to be ready....')
            
    def get_balance(self, exchange):
        return self.balances[exchange]
    
    def get_base_mappings(self, exchange):
        return self.base_mappings[exchange]
    
    def get_l2_stream(self, exchange, ticker):
        return self.l2_ticker_streams[exchange][ticker]
    
    async def search_overlap_asset(self, gateway, exchanges):   
        assets = {}
        
        for exchange in exchanges:
            tickers = await gateway.exchange.clients[exchange].get_all_mids()
            assets[exchange] = tickers
        
        binance_assets = {s[:-4] if s.endswith(('USDC', 'USDT')) else s for s in assets['binance']}
        hyperliquid_assets = set(assets['hyperliquid'].keys())
        overlapped = binance_assets & hyperliquid_assets
        self.overlapped_assets = sorted(list(overlapped))

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
                mappings = collections.defaultdict(list)
                assets = self.overlapped_assets
                            
                for k, v in perps_data.items():
                    if v['base_asset'] in assets and v['quote_asset'] in ['USDT', "USDC", "USD"]:
                        if 'PERP' in k or not any(x in k for x in ['-C', '-P']):
                            if not v.get('is_delisted', False):
                                funding_info = funding_data.get(k, {})  
                                if funding_info:
                                    combined_data = {**v, **funding_info, 'symbol':k}
                                    mappings[v['base_asset']].append(combined_data)
                remove = set()
                for k,v in mappings.items():
                    if len(v) > 1 and self.preference_quote is not None:
                        mappings[k] = [x for x in v if x['quote_asset'] in self.preference_quote]
                    if len(mappings[k]) ==0:
                        remove.add(k)
                for k in remove:
                    mappings.pop(k)
                    
                mappings = dict(sorted(mappings.items()))
                
                self.base_mappings[exchange] = mappings
                self.universe[exchange] = mappings
                self.ready_flags[exchange]['mappings'] = True
                await asyncio.sleep(60*4)
                
            except Exception as e:
                logging.error(f'Unknown error {e}')
                
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
        logging.info(f'[{exchange} Total ticker count: {len(tickers)}]')
        if exchange == 'binance': tickers.append('USDCUSDT')
        try:
            if len(tickers) >20:
                logging.info(f'[{exchange}] Using sequential calls for {len(tickers)} tickers to prevent IP blocking')
                lobs = []
                for ticker in tickers:
                    try:
                        lob = await gateway.executor.l2_book_mirror(
                            ticker=ticker,
                            speed_ms=500,
                            exc=exchange,
                            depth=20,
                            as_dict=False,
                            buffer_size=10
                        )
                        lobs.append(lob)
                        await asyncio.sleep(1)
                    except Exception as e:
                        lobs.append(e)
            else:
                logging.info(f'[{exchange}] Using parallel calls for {len(tickers)} tickers')
                lobs = await asyncio.gather(*[
                    gateway.executor.l2_book_mirror(
                        ticker=ticker,
                        speed_ms=500,
                        exc=exchange,
                        depth=20,
                        as_dict=False,
                        buffer_size=10
                    )for ticker in tickers
                ], return_exceptions=True)
            
            successful_pairs = []
            for ticker, lob in zip(tickers, lobs):
                if isinstance(lob, Exception):
                    logging.warning(f'[{exchange}] Failed to connect {ticker}: {lob}')
                else:
                    successful_pairs.append((ticker, lob))
            
            for ticker, lob in successful_pairs:
                self.l2_ticker_streams[exchange][ticker] = lob
            self.ready_flags[exchange]['streams'] = True
            logging.info(f"Updated L2 Streams for {exchange} and tickers: {list(self.l2_ticker_streams[exchange].keys())}")
        except Exception as e:
            logging.error(f'Error in performing executor l2_book_mirror: {e}')
                    
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
        