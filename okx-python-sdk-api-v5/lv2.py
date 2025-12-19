"""
OKX LV2 数据高级量化指标生成器
支持：订单簿不平衡、大单追踪、成交量剖面、订单流分析等
"""

"""
main
 ├── OKXLV2DataProcessor实例化
 ├── asyncio.create_task(processor.connect_to_okx(channels))
 │      └── connect_to_okx
 │           ├── 订阅频道
 │           └── 循环接收消息
 │                 └── process_message
 │                      ├── process_orderbook -> calculate_orderbook_metrics
 │                      ├── process_orderbook5 -> calculate_orderbook_metrics
 │                      ├── process_trades -> calculate_trade_flow_metrics
 │                      └── process_ticker
 ├── asyncio.create_task(display_indicators())
 │      └── display_indicators
 │           ├── processor.get_realtime_indicators
 │           └── processor.get_historical_metrics
 │                 ├── _calculate_volume_concentration
 │                 └── _calculate_momentum_score
"""

import asyncio
import websockets
import json
import pandas as pd
import numpy as np
from collections import deque, defaultdict
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Tuple, Optional
import pickle
import zlib
import base64
from itertools import islice


# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class OKXLV2DataProcessor:
    """OKX LV2数据处理和指标生成器"""

    def __init__(self, symbol: str = "SOL-USDT", max_depth: int = 20):
        """
        初始化LV2数据处理器

        Args:
            symbol: 交易对，例如 "BTC-USDT-SWAP"
            max_depth: 订单簿分析深度
        """
        self.symbol = symbol
        self.max_depth = max_depth

        # 订单簿数据存储
        self.orderbook = {
            'bids': {},  # price -> size
            'asks': {},  # price -> size
            'timestamp': None,
            'seq_id': None
        }

        # 交易数据存储
        self.trades = deque(maxlen=10000)  # 存储最近10000笔交易

        # 指标计算缓冲区
        self.tick_buffer = deque(maxlen=1000)

        # 指标存储
        self.metrics = {
            'orderbook_imbalance': deque(maxlen=1000),
            'buy_sell_pressure': deque(maxlen=1000),     #以上是orderbook5的数据解析的,写入

            'trade_flow': deque(maxlen=2000),        #在读数据后, calculate trade flow里面计算,  再写入的
            'volume_profile': defaultdict(float),   #trade  读数据的时候,做的直方图更新, 每条数据都更新
            'big_trades': deque(maxlen=5000),        #trade  大单才添加  读数据的时候,如果大单就添加
            'vwap_data': deque(maxlen=5000)         #trade  每条都添加 读数据的时候,就直接添加
        }

        # 统计信息
        self.stats = {
            'total_trades': 0,          #成交一笔算一笔
            'total_buy_volume': 0,      #买入多少算多少, 成交数据的价格 x 成交的量
            'total_sell_volume': 0,     #卖出多少算多少
            'last_update': None
        }

        # 配置
        self.big_trade_threshold = 10000  # 大单阈值（USDT）
        self.vwap_period = 300  # VWAP计算周期（秒）

    async def connect_to_okx(self, channels: List[str]):
        """连接到OKX WebSocket API"""
        uri = "wss://ws.okx.com:8443/ws/v5/public"

        async with websockets.connect(uri) as websocket:
            # 订阅频道
            for channel in channels:
                subscribe_msg = {
                    "op": "subscribe",
                    "args": [{"channel": channel, "instId": self.symbol}]
                }
                await websocket.send(json.dumps(subscribe_msg))
                logger.info(f"已订阅: {channel}")

            # 持续接收数据
            while True:
                try:
                    response = await websocket.recv()
                    data = json.loads(response)
                    await self.process_message(data)

                except Exception as e:
                    logger.error(f"处理数据时出错: {e}")
                    break

    async def process_message(self, data: dict):
        """处理接收到的消息"""
        if 'event' in data:
            # 处理订阅确认等事件
            if data['event'] == 'subscribe':
                logger.info(f"订阅成功: {data['arg']['channel']}")
            return

        channel = data.get('arg', {}).get('channel', '')

        if channel == 'books':
            # 订单簿数据
            await self.process_orderbook(data['data'])

        elif channel == 'trades':
            # 交易数据
            await self.process_trades(data['data'])

        elif channel == 'books5':
            # 5档深度数据
            await self.process_orderbook5(data['data'])

        elif channel == 'tickers':
            # ticker数据
            await self.process_ticker(data['data'])

    async def process_orderbook(self, data: List[dict]):
        """处理全深度订单簿数据"""
        if not data:
            return

        book_data = data[0]

        # 更新订单簿
        self.orderbook['bids'] = {float(price): float(size) for price, size in book_data['bids']}
        self.orderbook['asks'] = {float(price): float(size) for price, size in book_data['asks']}
        self.orderbook['timestamp'] = int(book_data['ts'])
        self.orderbook['seq_id'] = int(book_data['seqId'])

        # 计算订单簿指标
        await self.calculate_orderbook_metrics()

    async def process_orderbook5(self, data: List[dict]):
        """处理5档深度数据（更高效）"""
        if not data:
            return

        book_data = data[0]

        # 更新订单簿前5档
        self.orderbook['bids'] = {float(b[0]): float(b[1]) for b in book_data['bids']}
        self.orderbook['asks'] = {float(a[0]): float(a[1]) for a in book_data['asks']}
        self.orderbook['timestamp'] = int(book_data['ts'])
        # self.orderbook['ts'] = pd.to_datetime(int(book_data.get('ts', 0)), unit='ms').tz_localize('UTC').tz_convert('Asia/Shanghai')
        self.orderbook['ts'] = pd.to_datetime(int(book_data.get('ts', 0)), unit='ms').tz_localize('UTC').tz_convert('Asia/Shanghai')

        # 计算指标
        await self.calculate_orderbook_metrics()

    async def process_trades(self, data: List[dict]):
        """处理交易数据"""
        if not data:
            return

        current_time = datetime.now()

        for trade in data:
            # 解析交易数据
            trade_info = {
                'trade_id': trade.get('tradeId', ''),   #聚合的多笔交易中最新一笔交易的id
                'price': float(trade.get('px', 0)),     #成交价格
                'size': float(trade.get('sz', 0)),      #成交数量
                'side': trade.get('side', ''),  # 'buy' 或 'sell'
                'timestamp': int(trade.get('ts', 0)),
                'instId': trade.get('instId', ''),
                # 'ts': pd.to_datetime(int(trade.get('ts', 0)), unit='ms').tz_localize('UTC').tz_convert('Asia/Shanghai')
                'ts': pd.to_datetime(int(trade.get('ts', 0)), unit='ms')
            }

            # 添加到交易记录
            self.trades.append(trade_info)
            self.stats['total_trades'] += 1

            # 更新买卖量统计
            if trade_info['side'] == 'buy':
                self.stats['total_buy_volume'] += trade_info['size'] * trade_info['price']
            else:
                self.stats['total_sell_volume'] += trade_info['size'] * trade_info['price']

            # 检测大单
            if trade_info['size'] * trade_info['price'] >= self.big_trade_threshold:
                self.metrics['big_trades'].append(trade_info)

            # 更新成交量剖面
            price_level = round(trade_info['price'], 1)  # 按0.1精度聚合
            self.metrics['volume_profile'][price_level] += trade_info['size']

            # 更新VWAP数据
            self.metrics['vwap_data'].append(trade_info)

        #控制计算频率, 每秒最多计算一次
        if not hasattr(self, '_last_flow_calculation'):
            should_calculate = True
        else:
            time_since_last = (current_time - self._last_flow_calculation).total_seconds()
            should_calculate = time_since_last >= 0.0 #这里认为设定时间间隔

        if should_calculate and len(self.trades) >= 10:
            # 计算交易流指标
            await self.calculate_trade_flow_metrics()
            self._last_flow_calculation = current_time

    async def process_ticker(self, data: List[dict]):
        """处理ticker数据"""
        if data:
            ticker = data[0]
            # 可以在这里记录ticker信息或计算相关指标
            pass

    async def calculate_orderbook_metrics(self):
        """计算订单簿相关指标"""
        if not self.orderbook['bids'] or not self.orderbook['asks']:
            return

        bids = sorted(self.orderbook['bids'].items(), key=lambda x: x[0], reverse=True)
        asks = sorted(self.orderbook['asks'].items(), key=lambda x: x[0])

        # 1. 订单簿不平衡指标
        bid_volume = sum(size for _, size in bids[:self.max_depth])
        ask_volume = sum(size for _, size in asks[:self.max_depth])

        if bid_volume + ask_volume > 0:
            imbalance = (bid_volume - ask_volume) / (bid_volume + ask_volume)
        else:
            imbalance = 0

        imbalance_data = {
            'timestamp': self.orderbook['ts'],
            'imbalance': imbalance,
            'bid_volume': bid_volume,
            'ask_volume': ask_volume
        }
        #点单不平衡
        self.metrics['orderbook_imbalance'].append(imbalance_data)

        # 2. 加权价差
        if bids and asks:
            best_bid = bids[0][0]
            best_ask = asks[0][0]
            spread = best_ask - best_bid
            mid_price = (best_bid + best_ask) / 2

            # 计算加权买卖压力
            weighted_bid = sum(price * size for price, size in bids[:5])
            weighted_ask = sum(price * size for price, size in asks[:5])

            if sum(size for _, size in bids[:5]) > 0 and sum(size for _, size in asks[:5]) > 0:
                avg_bid = weighted_bid / sum(size for _, size in bids[:5])
                avg_ask = weighted_ask / sum(size for _, size in asks[:5])
                pressure = (mid_price - avg_bid) / (avg_ask - avg_bid) if avg_ask != avg_bid else 0.5
            else:
                pressure = 0.5

            pressure_data = {
                'timestamp': self.orderbook['ts'],
                'pressure': pressure,
                'spread': spread,
                'mid_price': mid_price
            }
            self.metrics['buy_sell_pressure'].append(pressure_data)

        # 3. 订单簿斜率（深度变化率）
        if len(bids) >= 5 and len(asks) >= 5:
            bid_prices = [price for price, _ in bids[:5]]
            bid_sizes = [size for _, size in bids[:5]]
            ask_prices = [price for price, _ in asks[:5]]
            ask_sizes = [size for _, size in asks[:5]]

            # 计算买卖深度斜率
            if len(bid_prices) > 1:
                bid_slope = np.polyfit(bid_prices, bid_sizes, 1)[0]
            else:
                bid_slope = 0

            if len(ask_prices) > 1:
                ask_slope = np.polyfit(ask_prices, ask_sizes, 1)[0]
            else:
                ask_slope = 0

            # 斜率差异可以作为市场情绪指标
            slope_diff = bid_slope - ask_slope

    async def calculate_trade_flow_metrics(self):
        """计算交易流指标"""
        if len(self.trades) < 10:
            return

        recent_trades = list(self.trades)[-100:]  # 取最近100笔交易
        latest_trade = recent_trades[-1]
        flow_timestamp = latest_trade.get('timestamp', 0)
        lastest_ts = self.trades[-1].get('ts', datetime.min) #最近一笔交易的时间

        # 1. 买卖压力指标
        buy_volume = sum(t['size'] for t in recent_trades if t['side'] == 'buy')
        sell_volume = sum(t['size'] for t in recent_trades if t['side'] == 'sell')

        if buy_volume + sell_volume > 0:
            buy_ratio = buy_volume / (buy_volume + sell_volume)
        else:
            buy_ratio = 0.5

        # 2. 大单净流入
        big_trades = [t for t in recent_trades if t['size'] * t['price'] >= self.big_trade_threshold]
        big_buy = sum(t['size'] for t in big_trades if t['side'] == 'buy')
        big_sell = sum(t['size'] for t in big_trades if t['side'] == 'sell')
        big_net = big_buy - big_sell

        # 3. 交易速度
        if len(recent_trades) >= 2:
            time_diff = (recent_trades[-1]['timestamp'] - recent_trades[0]['timestamp']) / 1000
            if time_diff > 0:
                trades_per_second = len(recent_trades) / time_diff
            else:
                trades_per_second = 0
        else:
            trades_per_second = 0

        # 4. 计算VWAP
        if self.metrics['vwap_data']:
            recent_vwap = list(self.metrics['vwap_data'])
            cutoff_time = datetime.now() - timedelta(seconds=self.vwap_period)
            # recent_vwap = [t for t in recent_vwap if t.get('ts', datetime.min).replace(tzinfo=None) > cutoff_time]

            cutoff_timestamp_ms = int(cutoff_time.timestamp() * 1000)  # 毫秒数
            recent_vwap = [t for t in recent_vwap if t.get('timestamp', 0) > cutoff_timestamp_ms]

            if recent_vwap:
                total_value = sum(t['price'] * t['size'] for t in recent_vwap)
                total_volume = sum(t['size'] for t in recent_vwap)
                vwap = total_value / total_volume if total_volume > 0 else 0
            else:
                vwap = 0
        else:
            vwap = 0

        flow_data = {
            'timestamp': flow_timestamp,
            # 'timestamp': lastest_ts,
            'buy_ratio': buy_ratio,         #最近100笔交易中,交易货币量中 主动买入/主动卖出
            'big_net_flow': big_net,        #超过1w的单子里面, 买入的币量 -  卖出的币量
            'trades_per_second': trades_per_second,     #最近的100笔, 平均每秒多少笔
            'vwap': vwap,                   #最近100笔交易中,加权平均成交价格,
            'buy_volume': buy_volume,       #所有买入的交易货币量
            'sell_volume': sell_volume      #所有卖出的交易货币量
        }

        #很傻比的写法, self.trades, self.metrics['vwap] 的数据, 在写入 self.metrics['trade_flow]
        #self.metrics里面已经有process_trade时候更新的,
        self.metrics['trade_flow'].append(flow_data)

        # 5. 订单流delta（主动买卖压力）
        if recent_trades:
            # 需要结合订单簿判断主动买卖
            # 简化版：通过价格与当前订单簿比较
            current_time = datetime.now().timestamp() * 1000
            for trade in recent_trades[-10:]:  # 只处理最近10笔
                if 'price' in trade and self.orderbook.get('asks') and self.orderbook.get('bids'):
                    price = trade['price']
                    best_bid = max(self.orderbook['bids'].keys()) if self.orderbook['bids'] else 0
                    best_ask = min(self.orderbook['asks'].keys()) if self.orderbook['asks'] else float('inf')

                    # 简单判断：如果成交价接近卖盘，可能是主动买
                    if best_ask > 0 and abs(price - best_ask) < abs(price - best_bid):
                        trade['aggressive'] = 'buy'
                    else:
                        trade['aggressive'] = 'sell'

    def get_realtime_indicators(self) -> Dict:
        """获取实时指标"""
        indicators = {}

        # 1. 订单簿不平衡
        if self.metrics['orderbook_imbalance']:
            latest_imbalance = self.metrics['orderbook_imbalance'][-1]
            indicators['orderbook_imbalance'] = latest_imbalance['imbalance']
            indicators['orderbook_strength'] = 'BULLISH' if latest_imbalance['imbalance'] > 0.1 else 'BEARISH' if \
            latest_imbalance['imbalance'] < -0.1 else 'NEUTRAL'
            indicators['bid_volume'] = latest_imbalance['bid_volume']
            indicators['ask_volume'] = latest_imbalance['ask_volume']
            indicators['timestamp'] = latest_imbalance['timestamp']

        # 2. 买卖压力
        if self.metrics['buy_sell_pressure']:
            latest_pressure = self.metrics['buy_sell_pressure'][-1]
            indicators['buy_pressure'] = latest_pressure['pressure']
            indicators['bid_ask_spread'] = latest_pressure['spread']

        #以上是由orderbook5的channel提供数据获得
######################################################################################################
        # 3. 交易流
        if self.metrics['trade_flow']:
            latest_flow = self.metrics['trade_flow'][-1]
            indicators['buy_ratio'] = latest_flow['buy_ratio']
            indicators['big_net_flow'] = latest_flow['big_net_flow']
            indicators['trade_per_sec'] = latest_flow['trades_per_second']
            indicators['vwap'] = latest_flow.get('vwap', 0)

            timestamp_ms = latest_flow.get('timestamp', 0)
            indicators['ts_trade'] = pd.to_datetime(timestamp_ms, unit='ms', utc=True).tz_convert('Asia/Shanghai')

            # 生成交易信号
            buy_signal = (indicators.get('orderbook_imbalance', 0) > 0.2 and
                          indicators.get('buy_ratio', 0.5) > 0.6 and
                          indicators.get('big_net_flow', 0) > 0)

            sell_signal = (indicators.get('orderbook_imbalance', 0) < -0.2 and
                           indicators.get('buy_ratio', 0.5) < 0.4 and
                           indicators.get('big_net_flow', 0) < 0)

            indicators['signal'] = 'BUY' if buy_signal else 'SELL' if sell_signal else 'NEUTRAL'

        # 4. 成交量剖面峰值
        if self.metrics['volume_profile']:
            if self.metrics['volume_profile']:
                max_volume_price = max(self.metrics['volume_profile'].items(), key=lambda x: x[1])
                indicators['poc_price'] = max_volume_price[0]  # 成交量最大价位
                indicators['poc_volume'] = max_volume_price[1]

        # 5. 大单统计
        if self.metrics['big_trades']:
            recent_big = list(self.metrics['big_trades'])[-20:]
            big_buys = sum(1 for t in recent_big if t.get('side') == 'buy')
            big_sells = sum(1 for t in recent_big if t.get('side') == 'sell')
            indicators['big_buy_ratio'] = big_buys / len(recent_big) if recent_big else 0

        return indicators

    def get_historical_metrics(self, lookback_period: int = 300) -> Dict:
        """获取历史指标（用于回测分析）"""
        if not self.metrics['trade_flow']:
            return {}

        # 获取指定时间窗口内的数据
        cutoff_time = datetime.now() - timedelta(seconds=lookback_period)
        recent_flow = [f for f in self.metrics['trade_flow']
                       if datetime.fromtimestamp(f['timestamp'] / 1000) > cutoff_time]

        if not recent_flow:
            return {}

        # 计算统计指标
        buy_ratios = [f['buy_ratio'] for f in recent_flow]


        #old
        # imbalances = [f.get('imbalance', 0) for f in self.metrics['orderbook_imbalance'][-len(recent_flow):]]

        # 使用itertools.islice获取最后n个元素
        n = len(recent_flow)
        deque_size = len(self.metrics['orderbook_imbalance'])

        if n > deque_size:
            n = deque_size

        if n > 0:
            # 获取最后n个元素
            recent_imbalances = list(islice(self.metrics['orderbook_imbalance'],
                                            deque_size - n, deque_size))
        else:
            recent_imbalances = []

        imbalances = [f.get('imbalance', 0) for f in recent_imbalances]







        return {
            'avg_buy_ratio': np.mean(buy_ratios) if buy_ratios else 0,
            'std_buy_ratio': np.std(buy_ratios) if buy_ratios else 0,
            'avg_imbalance': np.mean(imbalances) if imbalances else 0,
            'total_big_trades': len(self.metrics['big_trades']),
            'volume_concentration': self._calculate_volume_concentration(),
            'momentum_score': self._calculate_momentum_score()
        }

    def _calculate_volume_concentration(self) -> float:
        """计算成交量集中度"""
        if not self.metrics['volume_profile']:
            return 0

        total_volume = sum(self.metrics['volume_profile'].values())
        if total_volume == 0:
            return 0

        # 计算前20%价位的成交量占比
        sorted_prices = sorted(self.metrics['volume_profile'].items(),
                               key=lambda x: x[1], reverse=True)
        top_n = max(1, len(sorted_prices) // 5)  # 前20%
        top_volume = sum(v for _, v in sorted_prices[:top_n])

        return top_volume / total_volume

    def _calculate_momentum_score(self) -> float:
        """计算动量分数"""
        if len(self.metrics['trade_flow']) < 10:
            return 0

        recent_flows = list(self.metrics['trade_flow'])[-20:]

        # 基于多个指标计算动量
        scores = []

        # 1. 买盘比例趋势
        buy_ratios = [f['buy_ratio'] for f in recent_flows]
        if len(buy_ratios) >= 2:
            buy_trend = buy_ratios[-1] - buy_ratios[0]
            scores.append(buy_trend * 2)  # 权重2

        # 2. 大单净流入趋势
        big_flows = [f['big_net_flow'] for f in recent_flows]
        if big_flows:
            # 归一化处理
            max_flow = max(abs(f) for f in big_flows) if any(big_flows) else 1
            if max_flow > 0:
                big_trend = big_flows[-1] / max_flow
                scores.append(big_trend)

        # 3. 交易强度
        intensities = [f['trades_per_second'] for f in recent_flows]
        if intensities:
            intensity_trend = intensities[-1] - intensities[0]
            scores.append(intensity_trend / 10)  # 缩小尺度

        return sum(scores) / len(scores) if scores else 0

    def save_metrics(self, filename: str = "lv2_metrics.pkl"):
        """保存指标数据到文件"""
        with open(filename, 'wb') as f:
            pickle.dump({
                'metrics': self.metrics,
                'stats': self.stats,
                'symbol': self.symbol,
                'timestamp': datetime.now()
            }, f)
        logger.info(f"指标数据已保存到 {filename}")

    def load_metrics(self, filename: str = "lv2_metrics.pkl"):
        """从文件加载指标数据"""
        try:
            with open(filename, 'rb') as f:
                data = pickle.load(f)
                self.metrics = data['metrics']
                self.stats = data['stats']
            logger.info(f"已从 {filename} 加载指标数据")
        except FileNotFoundError:
            logger.warning(f"文件 {filename} 不存在")


    def print_queue_stats(self):
        """打印队列状态"""
        print(f"orderbook_imbalance队列长度: {len(self.metrics['orderbook_imbalance'])}")
        print(f"buy_sell_pressure队列长度: {len(self.metrics['buy_sell_pressure'])}")
        print(f"trade_flow队列长度: {len(self.metrics['trade_flow'])}")
        print(f"trades队列长度: {len(self.trades)}")
        print(f"vwap_data队列长度: {len(self.metrics['vwap_data'])}")
        print(f"big_trades队列长度: {len(self.metrics['big_trades'])}")









# 主程序
async def main():
    """主函数"""

    # 创建LV2数据处理器
    processor = OKXLV2DataProcessor(
        symbol="SOL-USDT",  # BTC永续合约
        max_depth=10
    )

    # 要订阅的频道
    channels = [
        "books5",  # 5档深度数据
        "trades",  # 交易数据
        # "tickers"  # ticker数据
    ]

    # 启动数据处理任务
    print("=" * 60)
    print("OKX LV2 量化指标生成器")
    print(f"交易对: {processor.symbol}")
    print(f"大单阈值: ${processor.big_trade_threshold}")
    print("=" * 60)

    # 启动数据连接
    try:
        # 注意：这里我们运行一个有限时间的示例
        # 实际使用时可以设置为无限循环

        # 创建任务
        data_task = asyncio.create_task(
            processor.connect_to_okx(channels)
        )

        # 同时运行指标显示任务
        async def display_indicators():
            while True:
                await asyncio.sleep(2)  # 每2秒更新一次

                indicators = processor.get_realtime_indicators()
                historical = processor.get_historical_metrics(lookback_period=60)
                processor.print_queue_stats()

                print("\n" + "=" * 60)
                print(f"实时指标 - {datetime.now().strftime('%H:%M:%S')}")
                print("-" * 60)

                if indicators:
                    for key, value in indicators.items():
                        if isinstance(value, float):
                            print(f"{key:20}: {value:8.4f}")
                        else:
                            print(f"{key:20}: {value}")

                if historical:
                    print("\n历史统计 (60秒):")
                    for key, value in historical.items():
                        if isinstance(value, float):
                            print(f"  {key:20}: {value:8.4f}")

                # 显示最新大单
                if processor.metrics['big_trades']:
                    recent_big = list(processor.metrics['big_trades'])[-10:]
                    print("\n最新大单:")
                    for trade in recent_big:
                        side = "买入" if trade.get('side') == 'buy' else "卖出"
                        amount = trade.get('size', 0) * trade.get('price', 0)
                        print(f"  {side}: {trade.get('price'):8.2f} × {trade.get('size'):6.4f} = ${amount:8.2f}")

        # 运行显示任务
        display_task = asyncio.create_task(display_indicators())


        # 等待一段时间后停止
        await asyncio.sleep(36000)  # 运行60秒示例

        # 保存数据
        # processor.save_metrics("okx_lv2_metrics.pkl")

    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        logger.error(f"程序运行出错: {e}")
    finally:
        # 清理资源
        print("\n程序结束")


# 简易使用示例
class OKXLV2IndicatorFactory:
    """LV2指标工厂 - 快速生成常用指标"""

    @staticmethod
    def create_momentum_indicator(processor: OKXLV2DataProcessor,
                                  window: int = 20) -> Dict:
        """创建动量指标"""
        if len(processor.metrics['trade_flow']) < window:
            return {}

        recent = list(processor.metrics['trade_flow'])[-window:]

        # 计算价格动量
        prices = [t.get('price', 0) for t in processor.trades][-window * 2:]
        if len(prices) >= 2:
            price_change = (prices[-1] - prices[0]) / prices[0] * 100
        else:
            price_change = 0

        # 计算成交量动量
        volumes = [t.get('buy_volume', 0) + t.get('sell_volume', 0) for t in recent]
        if volumes:
            volume_change = (volumes[-1] - volumes[0]) / volumes[0] * 100 if volumes[0] > 0 else 0
        else:
            volume_change = 0

        return {
            'price_momentum': price_change,
            'volume_momentum': volume_change,
            'combined_momentum': price_change * 0.7 + volume_change * 0.3
        }

    @staticmethod
    def create_market_depth_profile(processor: OKXLV2DataProcessor) -> Dict:
        """创建市场深度剖面图"""
        bids = processor.orderbook.get('bids', {})
        asks = processor.orderbook.get('asks', {})

        if not bids or not asks:
            return {}

        best_bid = max(bids.keys())
        best_ask = min(asks.keys())
        mid_price = (best_bid + best_ask) / 2

        # 计算各价位的累积深度
        bid_levels = sorted([(p, s) for p, s in bids.items() if p >= best_bid * 0.995],
                            reverse=True)
        ask_levels = sorted([(p, s) for p, s in asks.items() if p <= best_ask * 1.005])

        return {
            'mid_price': mid_price,
            'bid_depth': bid_levels[:10],  # 前10档买盘
            'ask_depth': ask_levels[:10],  # 前10档卖盘
            'total_bid_volume': sum(s for _, s in bid_levels),
            'total_ask_volume': sum(s for _, s in ask_levels)
        }


if __name__ == "__main__":
    # 运行主程序
    asyncio.run(main())