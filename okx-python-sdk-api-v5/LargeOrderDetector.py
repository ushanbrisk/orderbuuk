import pandas as pd
import numpy as np
from collections import defaultdict, deque
import matplotlib.pyplot as plt
from datetime import datetime
import json

class LargeOrderDetector:
    def __init__(self, size_threshold=50, time_window=2000):
        self.size_threshold = size_threshold
        self.time_window = time_window
        self.potential_splits = defaultdict(list)

    def detect_split_orders(self, current_ob, previous_ob, timestamp):
        """检测大单拆分模式"""
        if previous_ob is None:
            return []

        split_candidates = []

        # 分析买卖双方
        for side in ['bids', 'asks']:
            current_levels = getattr(current_ob, side)
            prev_levels = getattr(previous_ob, side)

            # 转换为字典便于查找
            current_dict = {level[0]: level for level in current_levels}
            prev_dict = {level[0]: level for level in prev_levels}

            for price, current_data in current_dict.items():
                if price in prev_dict:
                    prev_data = prev_dict[price]
                    size_change = current_data[1] - prev_data[1]

                    # 检测大额减少（可能是冰山订单的消耗）
                    if size_change < -self.size_threshold:
                        candidate = {
                            'timestamp': timestamp,
                            'side': 'ask' if side == 'asks' else 'bid',
                            'price': price,
                            'size_change': size_change,
                            'current_size': current_data[1],
                            'order_count_change': current_data[3] - prev_data[3],
                            'type': 'large_removal'
                        }
                        split_candidates.append(candidate)

                    # 检测大额新增（可能是拆分下单）
                    elif size_change > self.size_threshold:
                        candidate = {
                            'timestamp': timestamp,
                            'side': 'ask' if side == 'asks' else 'bid',
                            'price': price,
                            'size_change': size_change,
                            'current_size': current_data[1],
                            'order_count_change': current_data[3] - prev_data[3],
                            'type': 'large_addition'
                        }
                        split_candidates.append(candidate)

        # 聚类分析：在时间窗口内相同方向、相似价格的订单
        confirmed_splits = self._cluster_and_validate_splits(split_candidates)
        return confirmed_splits

    def _cluster_and_validate_splits(self, candidates):
        """聚类和验证拆分订单"""
        if not candidates:
            return []

        # 按方向分组
        bid_candidates = [c for c in candidates if c['side'] == 'bid']
        ask_candidates = [c for c in candidates if c['side'] == 'ask']

        confirmed = []

        for side_candidates in [bid_candidates, ask_candidates]:
            if len(side_candidates) < 2:
                continue

            # 按时间排序
            side_candidates.sort(key=lambda x: x['timestamp'])

            # 简单聚类：相同方向，价格接近，时间接近
            current_cluster = [side_candidates[0]]
            for i in range(1, len(side_candidates)):
                current = side_candidates[i]
                last = current_cluster[-1]

                time_diff = (current['timestamp'] - last['timestamp']).total_seconds() * 1000
                price_diff = abs(current['price'] - last['price']) / last['price']

                if time_diff < self.time_window and price_diff < 0.001:  # 0.1%价格区间
                    current_cluster.append(current)
                else:
                    if len(current_cluster) >= 2:
                        confirmed.extend(self._analyze_cluster_pattern(current_cluster))
                    current_cluster = [current]

            if len(current_cluster) >= 2:
                confirmed.extend(self._analyze_cluster_pattern(current_cluster))

        return confirmed

    def _analyze_cluster_pattern(self, cluster):
        """分析订单簇模式"""
        patterns = []

        # 检查是否是拆分模式（多个中等大小订单）
        sizes = [abs(c['size_change']) for c in cluster]
        avg_size = np.mean(sizes)
        std_size = np.std(sizes)

        if std_size / avg_size < 0.3:  # 大小相对均匀
            patterns.append({
                'pattern_type': 'split_order',
                'side': cluster[0]['side'],
                'total_size': sum(sizes),
                'order_count': len(cluster),
                'time_span_ms': (cluster[-1]['timestamp'] - cluster[0]['timestamp']).total_seconds() * 1000,
                'avg_price': np.mean([c['price'] for c in cluster])
            })

        return patterns