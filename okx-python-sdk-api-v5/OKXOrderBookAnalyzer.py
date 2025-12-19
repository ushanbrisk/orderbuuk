import pandas as pd
import numpy as np
from collections import defaultdict, deque
import matplotlib.pyplot as plt
from datetime import datetime
import json


class OKXOrderBookAnalyzer:
    def __init__(self, symbol="SOL-USDT", window_size=100):
        self.symbol = symbol
        self.orderbook_history = deque(maxlen=window_size)
        self.last_orderbook = None
        self.sequence_id = None

        # 大单检测参数
        self.large_order_threshold = 100  # SOL数量阈值
        self.time_window_ms = 2000  # 2秒时间窗口

        # 存储检测结果
        self.split_orders = []
        self.iceberg_orders = []
        self.order_flow = []

    def parse_websocket_message(self, message):
        """解析WebSocket消息"""
        try:
            # 提取时间戳和JSON数据
            timestamp_str, json_str = message.split('{', 1)
            json_str = '{' + json_str
            data = json.loads(json_str)

            # 提取订单簿数据
            if data.get('action') == 'update' and 'data' in data:
                orderbook_data = data['data'][0]
                timestamp = datetime.fromisoformat(timestamp_str.strip('Z').replace('T', ' '))

                return {
                    'timestamp': timestamp,
                    'asks': self._parse_levels(orderbook_data.get('asks', [])),
                    'bids': self._parse_levels(orderbook_data.get('bids', [])),
                    'ts': orderbook_data.get('ts'),
                    'seqId': orderbook_data.get('seqId'),
                    'prevSeqId': orderbook_data.get('prevSeqId')
                }
        except Exception as e:
            print(f"解析错误: {e}")
            return None

    def _parse_levels(self, levels):
        """解析价格档位数据 [价格, 数量, 未知, 订单数]"""
        return [[float(price), float(size), int(level[2]), int(level[3])]
                for level in levels for price, size, *rest in [level]]

    #
    # def parse_orderbook_data(self, orderbook_data):
    #     """解析OrderBookData对象（兼容新格式）"""
    #     return {
    #         'timestamp': orderbook_data.timestamp,
    #         'asks': orderbook_data.asks,
    #         'bids': orderbook_data.bids,
    #         'ts': str(int(orderbook_data.timestamp.timestamp() * 1000)),
    #         'seqId': orderbook_data.sequence_id,
    #         'prevSeqId': orderbook_data.sequence_id - 1,
    #         'action': orderbook_data.action
    #     }