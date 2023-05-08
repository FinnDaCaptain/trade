import requests
import json
import cupy as cp
import time
import threading
import datetime

api_url = "https://api.kucoin.com"
markets = ["BTC-USDT", "ETH-USDT", "LTC-USDT", "XRP-USDT", "ADA-USDT"]

class RateLimiter:
    def __init__(self, max_requests, time_window):
        self.max_requests = max_requests
        self.time_window = time_window
        self.request_times = []
        self.lock = threading.Lock()

    def acquire(self):
        with self.lock:
            current_time = time.time()
            self.request_times = [t for t in self.request_times if t > current_time - self.time_window]

            if len(self.request_times) < self.max_requests:
                self.request_times.append(current_time)
                return True
            else:
                return False

    def wait(self):
        while not self.acquire():
            time.sleep(0.1)

public_rate_limiter = RateLimiter(max_requests=500, time_window=10)  # Public endpoints: 500 requests per 10 seconds
private_rate_limiter = RateLimiter(max_requests=40, time_window=10)  # Private endpoints: Assuming 40 requests per 10 seconds (adjust accordingly)
hf_rate_limiter = RateLimiter(max_requests=1000, time_window=10)     # High-frequency endpoints: Assuming 1000 requests per 10 seconds (adjust accordingly)

def fetch_market_data(market, rate_limiter, interval='1min'):
    rate_limiter.wait()

    # Fetch historical klines (candlestick) data
    klines_endpoint = f"/api/v1/market/candles?type={interval}&symbol={market}"
    klines_response = requests.get(api_url + klines_endpoint)
    klines_data = json.loads(klines_response.text)

    rate_limiter.wait()

    # Fetch historical trades data
    trades_endpoint = f"/api/v1/market/histories?symbol={market}"
    trades_response = requests.get(api_url + trades_endpoint)
    trades_data = json.loads(trades_response.text)

    rate_limiter.wait()

    # Fetch level 3 order book data
    order_book_endpoint = f"/api/v1/market/orderbook/level3?symbol={market}"
    order_book_response = requests.get(api_url + order_book_endpoint)
    order_book_data = json.loads(order_book_response.text)

    return klines_data, trades_data, order_book_data

def save_data_as_cupy_array(data, market, interval='1min'):
    current_date = datetime.datetime.now().strftime('%Y%m%d')
    data_array = cp.array(data)
    file_name = f"{market}_{interval}_{current_date}_data.npy"
    cp.save(file_name, data_array)


num_markets = len(markets)
max_requests_per_market = public_rate_limiter.max_requests / num_markets
sleep_interval = public_rate_limiter.time_window / max_requests_per_market

def run_scraper(market, sleep_interval=sleep_interval, max_iterations=None, save_interval=10):
    data_points = []
    iterations = 0
    while max_iterations is None or iterations < max_iterations:
        klines_data, trades_data, order_book_data = fetch_market_data(market, public_rate_limiter)

        # Extract relevant data from the klines_data, trades_data, and order_book_data
        data_point = {
            "time": klines_data[0][0],
            "candlestick": klines_data[0],
            "trades": trades_data,
            "order_book": order_book_data
        }
        data_points.append(data_point)

        # Save data every save_interval iterations
        if (iterations + 1) % save_interval == 0:
            save_data_as_cupy_array(data_points, market)

        time.sleep(sleep_interval)
        iterations += 1

    # Save any remaining data after the loop has finished
    save_data_as_cupy_array(data_points, market)

scraping_threads = []

for market in markets:
    t = threading.Thread(target=run_scraper, args=(market,))
    t.start()
    scraping_threads.append(t)

for t in scraping_threads:
    t.join()
