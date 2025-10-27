# Extend implementation for funding_arbs repo (scan for every pair)
from data import MarketData, get_key
from quantpylib.gateway.master import Gateway
import asyncio
from datetime import datetime, timedelta
import pytz
from pprint import pprint
import logging
import time

exchanges =['binance', 'hyperliquid']

round_hr = lambda dt: (dt + timedelta(minutes=30)).replace(second=0, microsecond=0, minute=0)

async def calculate_cross_arbitrage(gateway, exchanges, asset):
    contracts_on_asset = {
        exchange: (len(market_data.get_base_mappings(exchange)[asset]) if asset in market_data.get_base_mappings(exchange) else 0)
        for exchange in exchanges
    }
    positions = {exc: pos for exc, pos in zip(exchanges, positions)}
    
    while True:
        contracts = []
        
        for exchange in exchanges:
            mapping = market_data.get_base_mappings(exchange)
            mapped = mapping.get(asset, [])
            
            for contract in mapped:
                lob = market_data.get_l2_stream(exchange, contract['symbol'])
                l2_last = {'ts': lob.timestamps, 'b': lob.bids, 'a': lob.asks}

                fx = 1
                l2_ts = l2_last['ts']
                bidX = l2_last['b'][0][0] * fx
                askX = l2_last['a'][0][0] * fx

                fr_ts = contract['timestamp']
                fr = float(contract['funding_rate'])
                frint = float(contract['funding_interval'])
                nx_ts = contract['next_funding']
                mark = float(contract['mark_price'])
                markX = mark * fx

                _interval_ms = frint * 60 * 60 * 1000

                print(f'frint: {frint}, _interval_ms: {_interval_ms}, fr: {fr}, nx_ts: {nx_ts}, fr_ts: {fr_ts}, l2_ts: {l2_ts}, mark: {mark}')

                now_ms = int(time.time() *1000)
                if nx_ts is None or fr_ts is None:
                    elapsed_ms = max(0, now_ms - (fr_ts or now_ms))
                    elapsed_ms = min(elapsed_ms, _interval_ms)
                    accrual = mark *fr * (elapsed_ms/_interval_ms if _interval_ms else 0)
                else:
                    accrual = mark *fr * (_interval_ms - max(0, nx_ts, fr_ts))/ _interval_ms if _interval_ms else 0

                accrualX = accrual * fx
                contracts.append({
                    'symbol': contract['symbol'],
                    'l2_ts': l2_ts,
                    'bidX': bidX,
                    'askX': askX,
                    'markX': markX,
                    'accrualX': accrualX,
                    'exchange': exchange,
                    'lob': lob,
                    'quantity_precision': contract['quantity_precision'],
                    'min_notional': contract['min_notional']
                })
                
        ticker_stats = {}
        for i in range(len(contracts)):
            for j in range(i+1, len(contracts)):
                icontract = contracts[i]
                jcontract = contracts[j]

                if icontract['exchange'] == jcontract['exchange']:
                    continue
                
                ij_ticker = (icontract['symbol'], icontract['exchange'], jcontract['symbol'], jcontract['exchange'])
               
async def main():
    global gateway, market_data
    calibrate_for = 2
    leverage = 5
    
    gateway = Gateway(config_keys=get_key())
    await gateway.init_clients()
    
    market_data = MarketData(
        gateway=gateway,
        exchanges=exchanges,
        preference_quote=['USDT', 'USDC']
    )
    
    assets = await market_data.search_overlap_asset(gateway=gateway, exchanges=exchanges)
    
    asyncio.create_task(market_data.serve_exchanges())
    await asyncio.sleep(20000)
    
    while True:
        now= datetime.now(pytz.utc)
        nearest_hr = round_hr(now)
        min_dist = (now.timestamp() - nearest_hr.timestamp())/60
        min_dist = abs(min_dist)
        
        if min_dist < calibrate_for:
            await asyncio.sleep(60)
        
        logging.info(f'Current time: {now}, Nearest hour: {nearest_hr}, Minutes to Nearest Hour: {min_dist}')
        
        try:
            await asyncio.gather(*[
                calculate_cross_arbitrage(
                    gateway=gateway,
                    exchanges=exchanges,
                    asset=asset
                ) for asset in assets
            ])
            pass
        except Exception as e:
            pass
        
    await gateway.cleanup_clients()


if __name__ == "__main__":
    asyncio.run(main())