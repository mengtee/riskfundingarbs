# Extend implementation for funding_arbs repo (scan for every pair)
from data import MarketData, get_key
from quantpylib.gateway.master import Gateway
import asyncio

from pprint import pprint

exchanges =['binance', 'hyperliquid']
async def main():
    global gateway, market_data
    
    gateway = Gateway(config_keys=get_key())
    await gateway.init_clients()
    
    market_data = MarketData(
        gateway=gateway,
        exchanges=exchanges,
        preference_quote=['USDT', 'USDC']
    )
    
    await market_data.search_overlap_asset(gateway=gateway, exchanges=exchanges)
    
    asyncio.create_task(market_data.serve_exchanges())
    await asyncio.sleep(20000)
    
    await gateway.cleanup_clients()


if __name__ == "__main__":
    asyncio.run(main())