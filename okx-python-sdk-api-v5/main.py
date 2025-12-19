import pandas as pd
import numpy as np
from collections import defaultdict, deque
import matplotlib.pyplot as plt
from datetime import datetime
import json
import asyncio

from OKXOrderBookAnalyzer import OKXOrderBookAnalyzer
from MarketVisualizer import MarketDepthVisualizer
from MarketVisualizer import OrderFlowAnalyzer
from LargeOrderDetector import LargeOrderDetector
from IceburgeOrderDetector import IcebergOrderDetector

from data_queue import global_orderbook_queue, OrderBookData

class RealTimeAnalysisSystem:
    def __init__(self):
        self.analyzer = OKXOrderBookAnalyzer()
        self.visualizer = MarketDepthVisualizer()
        self.flow_analyzer = OrderFlowAnalyzer()
        self.large_detector = LargeOrderDetector()
        self.iceberg_detector = IcebergOrderDetector()
        self.is_running = True
        self.processed_count = 0
        self.max_processing_rate = 10
        self.last_processed_time = 0

    def orderbook_data_to_dict(self, data: OrderBookData) -> dict:
        """将OrderBookData转换为字典格式"""
        return {
            'timestamp': data.timestamp,
            'bids': [[price, size, unknown, order_count]
                     for price, size, unknown, order_count in data.bids],
            'asks': [[price, size, unknown, order_count]
                     for price, size, unknown, order_count in data.asks],
            'symbol': data.symbol,
            'seqId': data.sequence_id,
            'action': data.action
        }

    def should_process(self, current_time: float) -> bool:
        """控制处理频率，避免过度渲染"""
        time_diff = current_time - self.last_processed_time
        if time_diff >= self.max_processing_rate / 1000.0:  # 转换为秒
            self.last_processed_time = current_time
            return True
        return False

    async def start_consumer(self):
        """启动数据消费者"""
        print("可视化消费者已启动，等待数据...")

        while self.is_running:
            try:
                # 非阻塞方式获取数据
                data = await asyncio.wait_for(
                    global_orderbook_queue.get(),
                    timeout=0.1  # 短暂超时，避免阻塞
                )

                if data and self.should_process(asyncio.get_event_loop().time()):
                    await self.process_message2(data)
                    self.processed_count += 1

                    # 每处理100条数据打印一次状态
                    if self.processed_count % 100 == 0:
                        print(f"已处理 {self.processed_count} 条订单簿数据")

                # 轻微延迟，避免CPU过度占用
                await asyncio.sleep(0.001)

            except asyncio.TimeoutError:
                # 超时是正常的，继续循环
                continue
            except Exception as e:
                print(f"数据处理错误: {e}")
                await asyncio.sleep(0.1)

    async def process_message2(self, orderbook_data: OrderBookData):
        """处理单条订单簿数据"""
        try:
            # 转换为字典格式
            data_dict = self.orderbook_data_to_dict(orderbook_data)

            # 存储历史数据
            self.analyzer.orderbook_history.append(data_dict)

            # 可视化当前订单簿
            title = orderbook_data.timestamp.strftime('%H:%M:%S.%f')[:-3]
            self.visualizer.plot_orderbook_snapshot(data_dict, title)

            # 打印简要信息
            bid_size = sum([b[1] for b in data_dict['bids'][:5]])
            ask_size = sum([a[1] for a in data_dict['asks'][:5]])
            print(f"📊 可视化更新 | 买5档: {bid_size:.1f} | 卖5档: {ask_size:.1f}")

        except Exception as e:
            print(f"消息处理错误: {e}")

    def stop(self):
        """停止系统"""
        self.is_running = False
        self.visualizer.close()
        print("可视化系统已停止")

    def process_message(self, websocket_message):
        """处理单条WebSocket消息"""
        orderbook = self.analyzer.parse_websocket_message(websocket_message)
        if not orderbook:
            return

        # 存储历史数据
        self.analyzer.orderbook_history.append(orderbook)

        # 可视化当前订单簿
        self.visualizer.plot_orderbook_snapshot(orderbook,
                                                orderbook['timestamp'].strftime('%H:%M:%S.%f'))

        plt.pause(30.0)

        # 如果有前一个快照，进行分析
        if len(self.analyzer.orderbook_history) > 1:
            prev_ob = self.analyzer.orderbook_history[-2]

            # 订单流分析
            flow_analysis = self.flow_analyzer.analyze_order_flow(orderbook, prev_ob)
            if flow_analysis:
                self._log_flow_analysis(flow_analysis)

            # 大单拆分检测
            split_orders = self.large_detector.detect_split_orders(
                orderbook, prev_ob, orderbook['timestamp']
            )
            if split_orders:
                self._log_split_orders(split_orders)

            # 冰山订单检测
            iceberg_orders = self.iceberg_detector.detect_iceberg_orders(orderbook, prev_ob)
            if iceberg_orders:
                self._log_iceberg_orders(iceberg_orders)

        self.analyzer.last_orderbook = orderbook

    def _log_flow_analysis(self, analysis):
        """记录订单流分析结果"""
        bid_pressure = len([c for c in analysis['bid_changes'] if c.get('is_large')])
        ask_pressure = len([c for c in analysis['ask_changes'] if c.get('is_large')])

        print(f"📊 订单流分析 | 时间: {analysis['timestamp']} | "
              f"买盘大单: {bid_pressure} | 卖盘大单: {ask_pressure}")

    def _log_split_orders(self, splits):
        """记录大单拆分检测结果"""
        for split in splits:
            print(f"🚨 大单拆分检测 | {split['side'].upper()}方 | "
                  f"总数量: {split['total_size']:.1f} | "
                  f"拆分单数: {split['order_count']} | "
                  f"时间: {split['time_span_ms']:.0f}ms")

    def _log_iceberg_orders(self, icebergs):
        """记录冰山订单检测结果"""
        for iceberg in icebergs:
            print(f"🧊 冰山订单检测 | {iceberg['side'].upper()}方 | "
                  f"价格: {iceberg['price']:.2f} | "
                  f"可见量: {iceberg['visible_size']:.1f} | "
                  f"置信度: {iceberg['confidence']:.1%}")


async def main():
    """主函数"""
    system = RealTimeAnalysisSystem()
    print("RealTimeAnalysisSystem created创建成功")
    try:
        # 启动消费者
        consumer_task = asyncio.create_task(system.start_consumer())

        # 保持运行，直到用户中断
        while system.is_running:
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        print("\n收到中断信号，正在关闭...")
    finally:
        system.stop()
        # 等待消费者任务结束
        if 'consumer_task' in locals():
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    # 运行主程序
    asyncio.run(main())



# # 使用示例
# if __name__ == "__main__":
#     system = RealTimeAnalysisSystem()
#
#     # 模拟处理您的WebSocket数据
#     sample_messages = [
#         '''2025-11-25T21:01:21.326Z{"arg":{"channel":"books","instId":"SOL-USDT"},"action":"update","data":[{"asks":[["136.25","142.200419","0","13"],["136.65","8.778232","0","7"],["136.67","7.577597","0","3"]],"bids":[["136.22","16.329923","0","5"],["136.21","53.632755","0","5"],["136.15","86.034611","0","11"],["136.14","110.116826","0","17"],["136.06","420.818524","0","15"],["135.82","3102.37531","0","10"],["135.8","1647.651084","0","15"]],"ts":"1764075681108","checksum":462577582,"seqId":27329772750,"prevSeqId":27329772739}]}''',
#         '''2025-11-25T21:01:21.432Z{"arg":{"channel":"books","instId":"SOL-USDT"},"action":"update","data":[{"asks":[["136.23","166.927082","0","14"],["136.24","99.709714","0","9"],["136.25","138.824273","0","12"],["136.28","161.391538","0","16"],["136.32","229.107154","0","14"],["136.33","134.788676","0","22"],["136.43","116.086566","0","10"],["140.23","0","0","0"]],"bids":[["136.23","0","0","0"],["136.21","43.118727","0","4"],["136.2","78.443444","0","12"],["136.19","176.4858","0","11"],["136.06","406.124524","0","14"],["135.98","120.65347","0","14"],["132.23","4.968064","0","24"]],"ts":"1764075681208","checksum":-553330731,"seqId":27329772800,"prevSeqId":27329772750}]}''',
#         '''2025-11-25T21:01:21.534Z{"arg":{"channel":"books","instId":"SOL-USDT"},"action":"update","data":[{"asks":[["136.24","97.341762","0","8"],["136.25","138.825472","0","13"],["136.26","133.260333","0","11"],["136.3","465.158811","0","21"],["136.32","229.104998","0","13"]],"bids":[["136.22","16.722365","0","6"],["136.17","87.25677","0","11"]],"ts":"1764075681308","checksum":1431226157,"seqId":27329772807,"prevSeqId":27329772800}]}''',
#         '''2025-11-25T21:01:21.634Z{"arg":{"channel":"books","instId":"SOL-USDT"},"action":"update","data":[{"asks":[["136.25","141.033618","0","14"],["136.27","178.254353","0","17"]],"bids":[["136.22","16.723365","0","7"],["136.06","415.819524","0","15"]],"ts":"1764075681408","checksum":1788020973,"seqId":27329772813,"prevSeqId":27329772807}]}'''
#         ]
#
#     for msg in sample_messages:
#         system.process_message(msg)