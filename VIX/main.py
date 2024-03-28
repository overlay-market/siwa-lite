import ccxt

from VIX.symbols import DerivativeSymbolsFetcher


def main(markets):
    for market in markets:
        if market not in ccxt.exchanges:
            raise ValueError(f"Exchange '{market}' is not supported by ccxt.")
        else:
            exchange = getattr(ccxt, market)()
            derived_markets_fetchers = DerivativeSymbolsFetcher(exchange)
            contracts_symbols = derived_markets_fetchers.fetch_symbols(market_type='all')
            print(f"Exchange: {market} Options: {contracts_symbols['options']}")

if __name__ == "__main__":
    markets = ["binance", "okx", "deribit"]
    try:
        main(markets)
    except ValueError as e:
        print(e)
