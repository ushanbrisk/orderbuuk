import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Tuple
import json

@dataclass
class SequenceValidationResult:
    """序列号验证结果"""
    is_valid: bool
    is_continuous: bool
    gap_size: int = 0
    expected_sequence_id: int = -1
    message: str = ""

@dataclass
class OrderBookData:
    """订单簿数据结构（增强版）"""
    timestamp: datetime
    bids: List[Tuple[float, float, int, int]]
    asks: List[Tuple[float, float, int, int]]
    symbol: str
    sequence_id: int
    prev_sequence_id: int
    action: str # 'snapshot' 或 'update'
    validation_result: Optional[SequenceValidationResult] = None  # 新增字段

class OrderBookQueue:
    """订单簿数据队列"""

    def __init__(self, max_size: int = 1000):
        self.queue = asyncio.Queue(maxsize=max_size)
        self.consumers = []
        self.is_running = True

    async def put(self, data: OrderBookData):
        """向队列放入数据"""
        if self.is_running:
            try:
                self.queue.put_nowait(data)
            except asyncio.QueueFull:
                # 队列满时丢弃最旧数据（如果需要保留最新数据）
                try:
                    self.queue.get_nowait()  # 丢弃一个旧数据
                    self.queue.put_nowait(data)  # 放入新数据
                except:
                    pass

    async def get(self) -> Optional[OrderBookData]:
        """从队列获取数据"""
        if self.is_running and not self.queue.empty():
            return await self.queue.get()
        return None

    def stop(self):
        """停止队列"""
        self.is_running = False

# 全局队列实例
global_orderbook_queue = OrderBookQueue()