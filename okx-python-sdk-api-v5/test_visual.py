import pandas as pd
import numpy as np
from collections import defaultdict, deque
import matplotlib.pyplot as plt
from datetime import datetime
import json
from MarketVisualizer import MarketDepthVisualizer


def test_visualization():
    """测试可视化功能"""
    visualizer = MarketDepthVisualizer()

    # 创建测试数据
    test_orderbook = {
        'bids': [
            [136.25, 175.637616, 0, 13],
            [136.24, 239.608138, 0, 15],
            [136.22, 341.985568, 0, 12],
            [136.20, 823.634876, 0, 15],
            [136.18, 150.123456, 0, 8],
            [136.15, 200.987654, 0, 10]
        ],
        'asks': [
            [136.33, 118.847807, 0, 14],
            [136.35, 125.201269, 0, 12],
            [136.37, 149.168512, 0, 13],
            [136.38, 174.761054, 0, 13],
            [136.40, 100.543210, 0, 9],
            [136.42, 180.123456, 0, 11]
        ],
        'timestamp': datetime.now()
    }

    # 多次更新测试动态效果
    for i in range(5):
        # 模拟数据变化
        test_orderbook['bids'][0][1] += i * 10  # 增加买盘数量
        test_orderbook['asks'][0][1] += i * 5  # 增加卖盘数量

        visualizer.plot_orderbook_snapshot(
            test_orderbook,
            f"Test Update {i + 1}"
        )

        # 等待一段时间看效果
        plt.pause(1.0)

    # 保持窗口打开
    input("按Enter键关闭窗口...")
    visualizer.close()


if __name__ == "__main__":
    test_visualization()