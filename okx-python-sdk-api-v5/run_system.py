import asyncio
import threading
import sys
import os
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
from datetime import datetime

from OKXOrderBookAnalyzer import OKXOrderBookAnalyzer
from MarketVisualizer import MarketDepthVisualizer
from MarketVisualizer import OrderFlowAnalyzer
from LargeOrderDetector import LargeOrderDetector
from IceburgeOrderDetector import IcebergOrderDetector

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from OKXWebSocketClient import OKXWebSocketClient
from data_queue import global_orderbook_queue

from main import main as consumer_main

#一条价格信息数据
@dataclass
class OrderBookLevel:
    price: float
    size: float
    unknown: int = 0
    order_count: int = 0

@dataclass
class FullOrderBook:
    #完整的订单簿状态
    timestamp: datetime
    bids: List[OrderBookLevel]  #买盘, 按价格降序排列
    asks: List[OrderBookLevel]  #卖盘, 按价格升序排列

    symbol: str
    sequence_id: int
    prev_sequence_id: int
    action: str

# async def run_producer():
#     """运行生产者（WebSocket数据获取）"""
#     print("启动WebSocket数据生产者...")
#     # await subscribe_without_login2(url, channels)
#
#
# async def run_consumer():
#     """运行消费者（可视化处理）"""
#     print("启动可视化数据消费者...")
#     # await consumer_main()
#
#
# async def main():
#     """同时运行生产者和消费者"""
#     producer_task = asyncio.create_task(run_producer())
#     consumer_task = asyncio.create_task(run_consumer())
#
#     # 等待任意任务完成（通常是因为错误或中断）
#     done, pending = await asyncio.wait(
#         [producer_task, consumer_task],
#         return_when=asyncio.FIRST_COMPLETED
#     )
#
#     # 取消未完成的任务
#     for task in pending:
#         task.cancel()
#         try:
#             await task
#         except asyncio.CancelledError:
#             pass
#
#
# if __name__ == "__main__":
#
#     try:
#         asyncio.run(main())
#     except KeyboardInterrupt:
#         print("\n系统已关闭")

class RealTimeAnalysisSystem:
    def __init__(self):
        self.symbol = "SOL-USDT"

        # 创建客户端实例，但不会自动启动
        self.ws_client = OKXWebSocketClient(
            api_key="e85e1598-5cca-4212-97b2-dbda96200c93",
            secret_key="5C44AE07D08C0A589037D29E10008467",
            passphrase="Hans428571",
            symbol=self.symbol
        )
        self.is_running = False
        self.window_size = 100


        self.current_bids: Dict[float, OrderBookLevel] = {}
        self.current_asks: Dict[float, OrderBookLevel] = {}
        self.max_levels = 400

        self.last_sequence_id: int = -1
        self.last_prev_sequence_id: int = -1

        self.snapshot_count = 0
        self.update_count = 0
        self.sequence_gap = 0

        #添加的处理组件
        #self.analyzer = OKXOrderBookAnalyzer()
        self.visualizer = MarketDepthVisualizer()
        self.flow_analyzer = OrderFlowAnalyzer()
        self.large_detector = LargeOrderDetector()
        # self.iceberg_detector = IcebergOrderDetector()

        #history
        self.orderbook_history = deque(maxlen=self.window_size)






    async def start_system(self):
        """启动整个系统"""
        self.is_running = True

        # 同时启动生产者和消费者
        producer_task = asyncio.create_task(self.ws_client.start())
        consumer_task = asyncio.create_task(self.start_consumer())

        try:
            await asyncio.gather(producer_task, consumer_task)
        except KeyboardInterrupt:
            print("\n系统关闭中...")
        finally:
            await self.stop_system()

    async def start_consumer(self):
        """启动数据消费者"""
        while self.is_running:
            try:
                data = await asyncio.wait_for(global_orderbook_queue.get(), timeout=0.1)
                if data:
                    await self.process_data(data)

            except asyncio.TimeoutError:
                continue

    async def process_data(self, data):

        action = getattr(data, 'action')
        if action == 'snapshot':
            full_bookdata = self._process_snapshot(data, action)
        elif action == 'update':
            full_bookdata =  self._process_update(data, action)
        else:
            print(f"未知的action类型: {action}")
            return None

        self.orderbook_history.append(full_bookdata)

        """处理数据"""
        # 您的可视化逻辑...
        # print(f"处理数据: {data}")

        self.visualizer.plot_orderbook_snapshot(full_bookdata)
        #
        # # 如果有前一个快照，进行分析
        # if len(self.orderbook_history) > 1:
        #     prev_ob = self.orderbook_history[-2]
        #
        #     # # 订单流分析
        #     # flow_analysis = self.flow_analyzer.analyze_order_flow(data, prev_ob)
        #     # if flow_analysis:
        #     #     self._log_flow_analysis(flow_analysis)
        #
        #     # 大单拆分检测
        #     split_orders = self.large_detector.detect_split_orders(
        #         data, prev_ob, data.timestamp
        #     )
        #     if split_orders:
        #         self._log_split_orders(split_orders)
        #
        #     # # 冰山订单检测
        #     # iceberg_orders = self.iceberg_detector.detect_iceberg_orders(data, prev_ob)
        #     # if iceberg_orders:
        #     #     self._log_iceberg_orders(iceberg_orders)

    def _process_snapshot(self, data, action):

        #清空数据, 用snapshot完全替换
        self.current_bids.clear()
        self.current_asks.clear()

        #解析买盘数据
        bids = getattr(data, 'bids')
        for bid in bids:
            price, size, unknown, order_count = bid[0], bid[1], bid[2], bid[3]
            self.current_bids[price] = OrderBookLevel(price, size, unknown, order_count)

        # 解析卖盘数据
        asks = getattr(data, 'asks')
        for ask in asks:
            price, size, unknown, order_count = ask[0], ask[1], ask[2], ask[3]
            self.current_asks[price] = OrderBookLevel(price, size, unknown, order_count)

        #更新序列号, 数据取完作为分界线,此时数据处理完了,所以当前seqId算作last_seqId
        self.last_sequence_id = getattr(data, 'sequence_id')
        self.last_prev_sequence_id = getattr(data, 'prev_sequence_id')
        self.snapshot_count += 1

        #构建全量订单簿
        timestamp = getattr(data, 'timestamp')
        return self._build_full_orderbook(timestamp, action)

    def _process_update(self, data, action):
        bids = getattr(data, 'bids')
        for bid_update in bids:
            self._apply_level_update(bid_update, self.current_bids, is_bid=True)

        asks = getattr(data, 'asks')
        for ask_update in asks:
            self._apply_level_update(ask_update, self.current_asks, is_bid=False)

        #更新序列号
        current_seq = getattr(data, 'sequence_id')
        self.last_prev_sequence_id = self.last_sequence_id
        self.last_sequence_id = current_seq
        self.update_count += 1

        #构建完整订单簿
        timestamp = getattr(data, 'timestamp')
        return self._build_full_orderbook(timestamp, action)

    def _apply_level_update(self, level_update: list, levels_dict: Dict[float, OrderBookLevel], is_bid:bool ):
        price, size, unknown, order_count = level_update

        if size == 0:
            levels_dict.pop(price, None)
        else:
            levels_dict[price] = OrderBookLevel(price, size, unknown, order_count)


    def _build_full_orderbook(self, timestamp: datetime, action:str):
        #构建完整的订单簿对象
        #排序, 买盘降序, 卖盘升序
        # 从大到小排列, 卖一, 卖二
        sorted_bids = sorted(self.current_bids.values(), key=lambda x: x.price, reverse=True)

        #从小到大排列, 买一, 买二
        sorted_asks = sorted(self.current_asks.values(), key=lambda x: x.price)

        #限制档位数
        if self.max_levels > 0:
            sorted_bids = sorted_bids[:self.max_levels]
            sorted_asks = sorted_asks[:self.max_levels]

        return FullOrderBook(
            timestamp=timestamp,
            bids=sorted_bids,
            asks=sorted_asks,
            symbol=self.symbol,
            sequence_id=self.last_sequence_id,
            prev_sequence_id=self.last_prev_sequence_id,
            action=action
        )








    async def stop_system(self):
        """停止系统"""
        self.is_running = False
        await self.ws_client.stop()

    def _log_split_orders(self, splits):
        """记录大单拆分检测结果"""
        # for split in splits:
        #     print(f"🚨 大单拆分检测 | {split['side'].upper()}方 | "
        #           f"价格: {split['price']:.1f} | "
        #           f"总数量: {split['size_change']:.1f} | "
        #           f"单数变化: {split['order_count_change']} | "
        #           # f"时间: {split['time_span_ms']:.0f}ms"
        #           )
    #
    # def _log_iceberg_orders(self, icebergs):
    #     """记录冰山订单检测结果"""
    #     for iceberg in icebergs:
    #         print(f"🧊 冰山订单检测 | {iceberg['side'].upper()}方 | "
    #               f"价格: {iceberg['price']:.2f} | "
    #               f"可见量: {iceberg['visible_size']:.1f} | "
    #               f"置信度: {iceberg['confidence']:.1%}")
    #





# 使用示例
async def main():
    system = RealTimeAnalysisSystem()
    await system.start_system()


if __name__ == "__main__":
    asyncio.run(main())

