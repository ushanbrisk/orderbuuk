import pandas as pd
import numpy as np
from collections import defaultdict, deque
import matplotlib.pyplot as plt
from datetime import datetime
import json

class MarketDepthVisualizer:
    def __init__(self):
        self.fig, (self.ax1, self.ax2) = plt.subplots(1, 2, figsize=(12, 10))
        plt.ion()

    def plot_orderbook_snapshot(self, orderbook, title=""):
        """绘制单个订单簿快照 - 修正版"""
        try:
            bids = orderbook.bids
            asks = orderbook.asks
            timestamp = orderbook.timestamp

            # 检查数据是否为空
            if not bids or not asks:
                print("警告: 订单簿数据为空")
                return

            # 提取价格和数量数据
            bid_prices = [getattr(level, 'price') for level in bids]
            bid_sizes = [getattr(level, 'size') for level in bids]
            ask_prices = [getattr(level, 'price') for level in asks]
            ask_sizes = [getattr(level, 'size') for level in asks]

            # 限制显示数量（前20档）
            display_levels = min(20, len(bid_prices), len(ask_prices))

            bid_prices = bid_prices[:display_levels]
            bid_sizes = bid_sizes[:display_levels]
            ask_prices = ask_prices[:display_levels]
            ask_sizes = ask_sizes[:display_levels]

############################################################################
            # # 动态计算高度：基于价格范围的百分比
            # all_prices = bid_prices + ask_prices
            # price_range = max(all_prices) - min(all_prices)
            #
            # # 方法1：基于价格范围的动态高度
            # if price_range > 0:
            #     dynamic_height = price_range * 0.03  # 价格范围的3%
            # else:
            #     dynamic_height = 0.01  # 默认值
            #
            # # 方法2：基于数据点数量的动态高度（更稳定）
            # total_levels = len(bid_prices) + len(ask_prices)
            # if total_levels > 0:
            #     height_by_count = (max(all_prices) - min(all_prices)) / total_levels * 0.8
            #     dynamic_height = max(dynamic_height, height_by_count)
            #
            # # 限制高度范围，避免过粗或过细
            # dynamic_height = max(0.005, min(dynamic_height, 0.05))
            #
            # print(f"动态高度: {dynamic_height:.4f}, 价格范围: {price_range:.4f}")

############################################################################

            # 合并所有价格并排序
            all_prices = sorted([getattr(b, 'price') for b in bids] + [getattr(a, 'price') for a in asks])

            # 计算平均价格间距
            if len(all_prices) > 1:
                price_gaps = [all_prices[i + 1] - all_prices[i] for i in range(len(all_prices) - 1)]
                avg_gap = np.mean(price_gaps) if price_gaps else 0.01
            else:
                avg_gap = 0.01

            # 设置高度为平均间距的80%（留出间隙）
            bar_height = avg_gap * 0.2
            dynamic_height = max(0.005, min(bar_height, 0.02))  # 限制范围

#####################################################################################################

            # 清除之前的图形
            self.ax1.clear()
            self.ax2.clear()

            # 1. 订单簿深度图
            if bid_prices and ask_prices:
                # 确保价格排序正确（买盘降序，卖盘升序）
                bid_prices_sorted = sorted(bid_prices, reverse=True)
                bid_sizes_sorted = [size for _, size in sorted(zip(bid_prices, bid_sizes), reverse=True)]

                ask_prices_sorted = sorted(ask_prices)
                ask_sizes_sorted = [size for _, size in sorted(zip(ask_prices, ask_sizes))]

                # 绘制水平条形图
                self.ax1.barh(bid_prices_sorted, bid_sizes_sorted,
                              height=dynamic_height, color='green', alpha=0.7, label='Bids')
                self.ax1.barh(ask_prices_sorted, ask_sizes_sorted,
                              height=dynamic_height, color='red', alpha=0.7, label='Asks')

                self.ax1.set_xlabel('Order Size')
                self.ax1.set_ylabel('Price')
                self.ax1.legend()
                self.ax1.set_title(f'Order Book Depth - {title}  {timestamp}')
                self.ax1.grid(True, alpha=0.3)

                # 自动调整坐标轴范围
                if bid_prices_sorted and ask_prices_sorted:
                    min_price = min(min(bid_prices_sorted), min(ask_prices_sorted))
                    max_price = max(max(bid_prices_sorted), max(ask_prices_sorted))
                    price_range = max_price - min_price

                    self.ax1.set_ylim(min_price - price_range * 0.1, max_price + price_range * 0.1)
                    self.ax1.set_xlim(0, max(max(bid_sizes_sorted), max(ask_sizes_sorted)) * 1.1)

            # 2. 买卖压力分析
            total_bid_size = sum(bid_sizes) if bid_sizes else 0
            total_ask_size = sum(ask_sizes) if ask_sizes else 0

            if total_ask_size > 0:
                pressure_ratio = total_bid_size / total_ask_size
            else:
                pressure_ratio = 1

            # 绘制压力比
            categories = ['Bid Pressure', 'Ask Pressure']
            values = [pressure_ratio, 1 / pressure_ratio if pressure_ratio > 0 else 1]
            colors = ['green', 'red']

            bars = self.ax2.bar(categories, values, color=colors, alpha=0.7, width=0.5)
            self.ax2.set_ylabel('Pressure Ratio')
            self.ax2.set_title(f'Market Pressure Ratio: {pressure_ratio:.3f}')
            self.ax2.grid(True, alpha=0.3)

            # 在柱状图上显示数值
            for bar, value in zip(bars, values):
                height = bar.get_height()
                self.ax2.text(bar.get_x() + bar.get_width() / 2., height,
                              f'{value:.2f}', ha='center', va='bottom')

            # 调整布局
            plt.tight_layout()

            # 刷新显示
            plt.draw()
            plt.pause(0.001)  # 短暂暂停以更新图形

            print(f"✓ 图形已更新: {title} | 买盘压力: {pressure_ratio:.3f}")

        except Exception as e:
            print(f"绘图错误: {e}")
            import traceback
            traceback.print_exc()

    def close(self):
        """关闭可视化"""
        plt.ioff()
        plt.close()
        print("可视化窗口已关闭")

class OrderFlowAnalyzer:
    def __init__(self):
        self.bid_flow = deque(maxlen=50)
        self.ask_flow = deque(maxlen=50)

    def analyze_order_flow(self, current_ob, previous_ob):
        """分析订单流变化"""
        if previous_ob is None:
            return None

        flow_analysis = {
            'timestamp': current_ob['timestamp'],
            'bid_changes': self._analyze_side_flow(current_ob['bids'], previous_ob['bids'], 'bid'),
            'ask_changes': self._analyze_side_flow(current_ob['asks'], previous_ob['asks'], 'ask'),
            'large_orders': []
        }

        return flow_analysis

    def _analyze_side_flow(self, current_levels, prev_levels, side):
        """分析单边订单流变化"""
        changes = []
        current_dict = {level[0]: level for level in current_levels}
        prev_dict = {level[0]: level for level in prev_levels}

        # 分析变化
        for price, current_level in current_dict.items():
            if price in prev_dict:
                prev_level = prev_dict[price]
                size_change = current_level[1] - prev_level[1]
                order_count_change = current_level[3] - prev_level[3]

                if abs(size_change) > 1:  # 忽略微小变化
                    changes.append({
                        'price': price,
                        'size_change': size_change,
                        'order_count_change': order_count_change,
                        'side': side,
                        'is_large': abs(size_change) > 50  # 大单阈值
                    })
            else:
                # 新增价格档位
                changes.append({
                    'price': price,
                    'size_change': current_level[1],
                    'order_count_change': current_level[3],
                    'side': side,
                    'is_new': True
                })

        return changes