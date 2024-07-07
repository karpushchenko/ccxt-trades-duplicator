import asyncio
import ccxt.async_support as ccxt
import logging
import sqlite3
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Exchange configuration
exchange_id = os.environ.get('EXCHANGE_ID')
api_key = os.environ.get('API_KEY')
secret = os.environ.get('SECRET')
password = os.environ.get('PASSWORD')

# Database setup
DB_NAME = 'trades.db'

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS processed_trades (
        ord_id TEXT PRIMARY KEY,
        symbol TEXT,
        amount REAL,
        side TEXT,
        processed_at TIMESTAMP
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS holdings (
        symbol TEXT PRIMARY KEY,
        amount REAL
    )
    ''')
    conn.commit()
    conn.close()

def is_trade_processed(ord_id):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM processed_trades WHERE ord_id = ?", (ord_id,))
    result = cursor.fetchone() is not None
    conn.close()
    return result

def mark_trade_as_processed(ord_id, symbol, amount, side):
    logger.info(f"Marking trade as processed: {ord_id}, {symbol}, {amount}, {side}")
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO processed_trades (ord_id, symbol, amount, side, processed_at) VALUES (?, ?, ?, ?, ?)",
        (ord_id, symbol, amount, side, datetime.now())
    )
    conn.commit()
    conn.close()

def update_holdings(symbol, amount, side):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT amount FROM holdings WHERE symbol = ?", (symbol,))
    result = cursor.fetchone()
    if result:
        current_amount = result[0]
        new_amount = current_amount + amount if side == 'buy' else current_amount - amount
        cursor.execute("UPDATE holdings SET amount = ? WHERE symbol = ?", (new_amount, symbol))
    else:
        cursor.execute("INSERT INTO holdings (symbol, amount) VALUES (?, ?)", (symbol, amount))
    conn.commit()
    conn.close()

def get_holding_amount(symbol):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT amount FROM holdings WHERE symbol = ?", (symbol,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else 0

async def get_usdt_balance(exchange):
    balance = await exchange.fetch_balance()
    usdt_balance = balance['USDT']['free']
    logger.info(f"Available USDT balance: {usdt_balance}")
    return usdt_balance

async def fetch_and_copy_trades():
    exchange = getattr(ccxt, exchange_id)({
        'apiKey': api_key,
        'secret': secret,
        'password': password,
        'enableRateLimit': True,
        'options': {'defaultType': 'spot'}
    })

    try:
        logger.info("Connecting to the exchange...")
        await exchange.load_markets()
        logger.info("Connected successfully. Fetching trades...")

        while True:
            try:
                usdt_balance = await get_usdt_balance(exchange)
                order_amount_usdt = usdt_balance * 0.1  # 10% of available balance

                account = await exchange.fetch_accounts()
                print(f"Account: {account}")

                trades = await exchange.fetch_my_trades(symbol=None, since=None, limit=10, params={})
                
                for trade in reversed(trades):
                    ord_id = trade['order']
                    
                    if is_trade_processed(ord_id):
                        continue  # Skip already processed trades
                    
                    logger.info(f"Processing trade: {trade}")

                    symbol = trade['symbol']
                    side = trade['side']
                    market = exchange.market(symbol)

                    if market['quote'] != 'USDT':
                        logger.warning(f"Skipping non-USDT market: {symbol}")
                        continue

                    try:
                        if side == 'buy':
                            price = trade['price']
                            amount = order_amount_usdt / price
                            logger.info(f"Attempting to place new buy order for {amount} {symbol} (value: {order_amount_usdt} USDT)")
                            new_order = await exchange.create_market_order(symbol, side, amount, params={'tdMode': 'spot_isolated'})
                            logger.info(f"Placed new buy lead trade: {new_order}")
                            # Mark the Lead trade as processed to ignore it in the future
                            mark_trade_as_processed(new_order['id'], symbol, amount, side)
                            update_holdings(symbol, amount, 'buy')
                        elif side == 'sell':
                            amount_to_sell = get_holding_amount(symbol)
                            if amount_to_sell > 0:
                                logger.info(f"Attempting to sell all holdings of {symbol}: {amount_to_sell}")
                                margin_mode = await exchange.set_margin_mode('spot_isolated', symbol)
                                logger.info(f"Margin mode: {margin_mode}")
                                new_order = await exchange.create_market_order(symbol, side, amount_to_sell, params={'tdMode': 'spot_isolated'})
                                logger.info(f"Placed new sell lead trade: {new_order}")
                                # Mark the Lead trade as processed to ignore it in the future
                                mark_trade_as_processed(new_order['id'], symbol, amount_to_sell, side)
                                update_holdings(symbol, amount_to_sell, 'sell')
                            else:
                                logger.info(f"No holdings to sell for {symbol}")
                        
                        mark_trade_as_processed(ord_id, symbol, amount, side)
                    except ccxt.InsufficientFunds as e:
                        logger.error(f"Insufficient funds to place order: {e}")
                    except ccxt.ExchangeError as e:
                        logger.error(f"Exchange error when placing order: {e}")
                    except Exception as e:
                        logger.error(f"Unexpected error placing new trade: {e}")

                await asyncio.sleep(10)  # Adjust based on your needs and API rate limits

            except ccxt.NetworkError as e:
                logger.error(f"Network error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            except ccxt.ExchangeError as e:
                logger.error(f"Exchange error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Unexpected error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)

    except KeyboardInterrupt:
        logger.info("Stopping the trade fetcher...")
    finally:
        await exchange.close()

if __name__ == "__main__":
    init_db()
    asyncio.run(fetch_and_copy_trades())