# Extend implementation for funding_arbs repo (scan for every pair)
from dotenv import load_dotenv
load_dotenv()

from data import MarketData, get_key
from quantpylib.gateway.master import Gateway
import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
import pytz
from pprint import pprint
import logging
import time
import numpy as np
from quant_telegram import TelegramBot

trade_fees = {
    'binance': [0.0002, 0.0005], # maker, taker
    'hyperliquid': [0.00015, 0.00045]
}

exchanges =['binance', 'hyperliquid']

funding_opportunities = {}
fr_annualised = {}
last_tg_alert = 0

round_hr = lambda dt: (dt + timedelta(minutes=30)).replace(second=0, microsecond=0, minute=0)
interval_per_year = lambda hours: ((24/ hours) *365)

async def calculate_cross_arbitrage(timeout, **kwargs):
    try:
        await asyncio.wait_for(_calculate_cross_arbitrage(**kwargs), timeout=timeout)
    except asyncio.TimeoutError:
        logging.warning(f"Trade for {kwargs.get('asset')} timed out after {timeout} seconds")

async def _calculate_cross_arbitrage(gateway, exchanges, asset):
    contracts_on_asset = {
        exchange: (len(market_data.get_base_mappings(exchange)[asset]) if asset in market_data.get_base_mappings(exchange) else 0)
        for exchange in exchanges
    }
    
    evs = {}
    skip = set()
    
    while True:
        contracts = []
        
        for exchange in exchanges:
            mapping = market_data.get_base_mappings(exchange)
            mapped = mapping.get(asset, [])
                        
            for contract in mapped:
                
                lob = market_data.get_l2_stream(exchange, contract['symbol'])
                l2_last = {'ts': lob.timestamp, 'b': lob.bids, 'a': lob.asks}

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
                    accrual = mark *fr * (_interval_ms - max(0, nx_ts - fr_ts))/ _interval_ms if _interval_ms else 0

                accrualX = accrual * fx
                fr_annual = fr * interval_per_year(frint) *100
                
                contracts.append({
                    'symbol': contract['symbol'],
                    'l2_ts': l2_ts,
                    'bidX': bidX,
                    'askX': askX,
                    'markX': markX,
                    'accrualX': accrualX,
                    'fr_annual': fr_annual,
                    'exchange': exchange,
                    'lob': lob,
                    'quantity_precision': contract['quantity_precision'],
                    'min_notional': contract['min_notional']
                })
        
        # This will only compared one asset contract (the input was one asset for this function call)        
        arbs_stats = {}
        for i in range(len(contracts)):
            for j in range(i+1, len(contracts)):
                icontract = contracts[i]
                jcontract = contracts[j]

                if icontract['exchange'] == jcontract['exchange']:
                    continue
                
                ij_ticker = (icontract['symbol'], icontract['exchange'], jcontract['symbol'], jcontract['exchange'])
                ji_ticker = (jcontract['symbol'], jcontract['exchange'], icontract['symbol'], icontract['exchange'])
                
                if ij_ticker in skip or ji_ticker in skip:
                    continue
                
                l2_delay = int(time.time()* 1000) - min(icontract['l2_ts'], jcontract['l2_ts'])
                if l2_delay > 4000:
                    skip.add(ij_ticker)
                    skip.add(ji_ticker)
                    continue
                
                ij_basis = jcontract['bidX'] - icontract['askX']
                ij_basis_adj = ij_basis + (icontract['accrualX'] - jcontract['accrualX'])
                
                ji_basis = icontract['bidX'] - jcontract['askX']
                ji_basis_adj = ji_basis + (jcontract['accrualX'] - icontract['accrualX'])
                
                avg_mark = 0.5 * (icontract['markX'] + jcontract['markX'])
                fees = np.max(
                    trade_fees[icontract['exchange']][0] + trade_fees[jcontract['exchange']][1],
                    trade_fees[jcontract['exchange']][0] + trade_fees[icontract['exchange']][1]
                )
                inertia = fees * avg_mark
                
                # Instant profit percentage
                ij_ev = (ij_basis_adj - inertia) / avg_mark
                ji_ev = (ji_basis_adj - inertia) / avg_mark
                
                # Calculate the annualised funding rate differential
                fr_diff_ij = icontract['fr_annual'] - jcontract['fr_annual']
                fr_diff_ji = jcontract['fr_annual'] - icontract['fr_annual']
                
                if fr_diff_ij >= fr_diff_ji:
                    arbs_stats[ij_ticker] = [fr_diff_ij, ij_ev]
                else:
                    arbs_stats[ji_ticker] = [fr_diff_ji, ji_ev]
        if arbs_stats:
            global funding_opportunities
            funding_opportunities.update(arbs_stats)
        
        iter += 1
        if iter % 15 == 0:
            await asyncio.sleep(1)
            continue
                   
async def main():
    global gateway, market_data
    calibrate_for = 2
    leverage = 5
    
    bot = TelegramBot()
    
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
                    timeout=30,
                    gateway=gateway,
                    exchanges=exchanges,
                    asset=asset
                ) for asset in assets
            ])
            
            if funding_opportunities:
                sorted_opportunities = dict(sorted(
                    funding_opportunities.items(),
                    key=lambda x:x[1][0],
                    reverse=True
                ))
            
            current_time = time.time()
            if current_time - last_tg_alert > 300 and sorted_opportunities:
                # Find the best opportunity
                best_opportunity = next(iter(sorted_opportunities.items()))
                (contract_i, exchange_i, contract_j, exchange_j), (fr_diff, ev) = best_opportunity
                
                if fr_diff > 15.0:  # High threshold for emergency alerts
                    await bot.emergency_alert(
                        f"🚨 HIGH FUNDING ARBITRAGE: {fr_diff:.1f}% annualized spread detected!",
                        action_required=f"Check funding rates on {exchange_i} vs {exchange_j}"
                    )
                elif fr_diff > 8.0:  # Regular alerts for good opportunities  
                    await bot.custom_message(
                        f"💡 Funding opportunity detected:\n"
                        f"📊 {fr_diff:.1f}% annualized spread\n"
                        f"💰 {ev*100:.2f}% instant EV\n"
                        f"Long: {exchange_i} | Short: {exchange_j}",
                        throttle_seconds=300
                    )
                
                last_tg_alert = current_time
            
        except Exception as e:
            print(f'Calculations errors: {e}')
        
    await bot.cleanup()
    await gateway.cleanup_clients()


if __name__ == "__main__":
    asyncio.run(main())