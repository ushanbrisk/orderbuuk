import pandas as pd
import numpy as np
from collections import defaultdict, deque
import matplotlib.pyplot as plt
from datetime import datetime
import json

class IcebergOrderDetector:
    def __init__(self):
        self.price_level_behavior = defaultdict(lambda: deque(maxlen=10))

    def detect_iceberg_orders(self, current_ob, previous_ob):
        """检测冰山订单"""
        if previous_ob is None:
            return []

        iceberg_candidates = []

        for side in ['bids', 'asks']:
            # current_levels = current_ob[side]
            current_levels = getattr(current_ob, side)
            prev_levels = getattr(previous_ob, side)

            current_dict = {level[0]: level for level in current_levels}
            prev_dict = {level[0]: level for level in prev_levels}

            for price, current_data in current_dict.items():
                if price in prev_dict:
                    prev_data = prev_dict[price]

                    # 关键特征：订单数量减少后又迅速恢复
                    size_change = current_data[1] - prev_data[1]
                    order_count_change = current_data[3] - prev_data[3]

                    # 记录价格档位行为
                    behavior = {
                        'timestamp': getattr(current_ob, 'timestamp'),
                        'size': current_data[1],
                        'previous_size': prev_data[1],
                        'order_count': current_data[3],
                        'size_change': size_change
                    }

                    self.price_level_behavior[price].append(behavior)

                    # 检测冰山订单模式
                    if self._is_iceberg_pattern(price, side):
                        iceberg_candidates.append({
                            'pattern_type': 'iceberg',
                            'side': 'ask' if side == 'asks' else 'bid',
                            'price': price,
                            'visible_size': current_data[1],
                            'order_count': current_data[3],
                            'confidence': self._calculate_iceberg_confidence(price)
                        })

        return iceberg_candidates

    def _is_iceberg_pattern(self, price, side):
        """判断是否为冰山订单模式"""
        history = list(self.price_level_behavior[price])
        if len(history) < 3:
            return False

        # 检查重置模式：数量减少后迅速恢复
        reset_count = 0
        for i in range(1, len(history)):
            current = history[i]
            previous = history[i - 1]

            # 典型冰山特征：数量被消耗后迅速补充
            if (previous['size'] > current['previous_size'] and  # 之前被消耗
                    abs(current['size'] - previous['size']) < previous['size'] * 0.1):  # 迅速恢复
                reset_count += 1

        return reset_count >= 2