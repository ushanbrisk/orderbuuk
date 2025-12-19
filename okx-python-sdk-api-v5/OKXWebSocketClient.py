import asyncio
import base64
from datetime import datetime
import hmac
import json
import time
import zlib
import requests
import websockets
from data_queue import OrderBookData, global_orderbook_queue, SequenceValidationResult


class OKXWebSocketClient:
    def __init__(self, api_key="", secret_key="", passphrase="", symbol="SOL-USDT"):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.symbol = symbol
        self.url = "wss://ws.okx.com:8443/ws/v5/public"
        self.channels = [{"channel": "books", "instId": self.symbol}]

        self.is_running = False
        self.ws_connection = None

        # 新增：序列号验证相关属性
        self.last_sequence_id: int = -1
        self.last_prev_sequence_id: int = -1
        self.sequence_validation_enabled: bool = True
        self.sequence_gap_threshold: int = 10  # 允许的最大序列号间隔
        self.sequence_errors_count: int = 0
        self.max_sequence_errors: int = 5  # 最大允许的错误次数
        self.max_gap = 500
        # 统计信息
        self.stats = {
            'total_messages': 0,
            'valid_messages': 0,
            'sequence_gaps': 0,
            'resets_detected': 0
        }

    def get_timestamp(self):
        now = datetime.now()
        t = now.isoformat("T", "milliseconds")
        return t + "Z"

    def validate_sequence_ids(self, current_seq, prev_seq, action):
        """针对OKX特性的序列号验证"""

        if action == 'snapshot':
            # 快照数据，重置状态
            self.last_seq_id = current_seq
            return {"status": "snapshot_reset", "gap": 0, "is_valid": True}

        # OKX的prevSeqId应该严格匹配last_seq_id
        if prev_seq != self.last_seq_id:
            gap = current_seq - (self.last_seq_id + 1)

            if 0 < gap <= self.max_gap:
                # 小幅度跳跃，在OKX中是正常的
                print(f"OKX正常跳跃: 丢失{gap}条消息（可能是其他频道数据）")
                self.last_seq_id = current_seq
                return {"status": "acceptable_gap", "gap": gap, "is_valid": True}
            elif gap > self.max_gap:
                # 大幅度跳跃，可能需要关注
                print(f"⚠️ 较大跳跃: {gap}条，但可能在OKX正常范围内")
                self.last_seq_id = current_seq
                return {"status": "large_gap", "gap": gap, "is_valid": True}
            else:
                # 序列号回退（真正的问题）
                return {"status": "sequence_rollback", "gap": gap, "is_valid": False}
        else:
            # 完美连续
            self.last_seq_id = current_seq
            return {"status": "continuous", "gap": 0, "is_valid": True}




    def update_sequence_state(self, current_sequence_id: int, prev_sequence_id: int,
                              validation_result: SequenceValidationResult):
        """更新序列号状态"""
        if validation_result['is_valid']:
            self.last_sequence_id = current_sequence_id
            self.last_prev_sequence_id = prev_sequence_id
            self.sequence_errors_count = 0  # 重置错误计数
        else:
            self.sequence_errors_count += 1

            # 如果错误次数过多，可能需要重新订阅
            if self.sequence_errors_count >= self.max_sequence_errors:
                print(f"⚠️ 序列号错误次数过多({self.sequence_errors_count})，建议重新订阅")

        self.stats['total_messages'] += 1
        if validation_result['is_valid']:
            self.stats['valid_messages'] += 1

    def should_reconnect(self) -> bool:
        """判断是否需要重新连接"""
        return self.sequence_errors_count >= self.max_sequence_errors

    def get_sequence_stats(self):
        """获取序列号统计信息"""
        return {
            **self.stats,
            'last_sequence_id': self.last_sequence_id,
            'sequence_errors_count': self.sequence_errors_count,
            'validation_enabled': self.sequence_validation_enabled
        }

    def convert_okx_ts(self, ts_milliseconds):
        """转换OKX的ts字段为ISO格式"""
        timestamp_seconds = ts_milliseconds / 1000.0
        dt = datetime.fromtimestamp(timestamp_seconds)
        return dt.isoformat("T", "milliseconds") + "Z"

    # def compare_timestamps(self, okx_ts):
    #     """比较OKX时间和本地时间"""
    #     okx_time = self.convert_okx_ts(okx_ts)
    #     local_time = TimestampConverter.get_local_timestamp()
    #
    #     # 计算时间差（秒）
    #     okx_dt = datetime.fromisoformat(okx_time.replace('Z', ''))
    #     local_dt = datetime.fromisoformat(local_time.replace('Z', ''))
    #     time_diff = (local_dt - okx_dt).total_seconds()
    #
    #     return {
    #         'okx_time': okx_time,
    #         'local_time': local_time,
    #         'time_difference_seconds': abs(time_diff),
    #         'is_okx_ahead': time_diff < 0
    #     }

    def get_server_time(self):
        url = "https://www.okx.com/api/v5/public/time"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()['data'][0]['ts']
        else:
            return ""

    def get_local_timestamp(self):
        return int(time.time())

    def login_params(self, timestamp, api_key, passphrase, secret_key):
        message = timestamp + 'GET' + '/users/self/verify'
        mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        d = mac.digest()
        sign = base64.b64encode(d)

        login_param = {"op": "login", "args": [{"apiKey": api_key,
                                                "passphrase": passphrase,
                                                "timestamp": timestamp,
                                                "sign": sign.decode("utf-8")}]}
        return json.dumps(login_param)

    def parse_websocket_message_to_dataclass(self, message):
        """将WebSocket消息解析为OrderBookData（增强版）"""
        try:
            # 提取时间戳和JSON数据
            if 'Z{' not in message:
                return None

            timestamp_str, json_str = message.split('{', 1)
            json_str = '{' + json_str
            data = json.loads(json_str)

            if 'data' not in data or not data['data']:
                return None

            orderbook_data = data['data'][0]
            timestamp_okx = self.convert_okx_ts(int(orderbook_data.get('ts', 0)))

            # 提取序列号
            current_sequence_id = orderbook_data.get('seqId', 0)
            prev_sequence_id = orderbook_data.get('prevSeqId', -1)
            action = data.get('action', 'update')
            print(f"{timestamp_okx}, {prev_sequence_id}, {current_sequence_id}, {action} ")

            # 验证序列号
            if self.sequence_validation_enabled:
                validation_result = self.validate_sequence_ids(current_sequence_id, prev_sequence_id, action)
                self.update_sequence_state(current_sequence_id, prev_sequence_id, validation_result)

                # 记录验证结果
                if not validation_result['is_valid']:
                    print(f"🔍 序列号验证: {validation_result.message}")
                    print(
                        f"   当前: {current_sequence_id}, 前一个: {prev_sequence_id}, 上一条: {self.last_sequence_id}")

                # 如果验证失败严重，返回None或特殊处理
                if not validation_result['is_valid'] and self.sequence_errors_count >= 3:
                    print("⚠️ 序列号验证失败次数过多，跳过此消息")
                    return None

            # 转换为OrderBookData
            return OrderBookData(
                # timestamp=datetime.fromisoformat(timestamp_str.strip('Z').replace('T', ' ')),
                timestamp=datetime.fromisoformat(timestamp_okx.strip('Z').replace('T', ' ')),

                bids=[(float(level[0]), float(level[1]), int(level[2]), int(level[3]))
                      for level in orderbook_data.get('bids', [])],
                asks=[(float(level[0]), float(level[1]), int(level[2]), int(level[3]))
                      for level in orderbook_data.get('asks', [])],
                symbol=data['arg']['instId'],
                sequence_id=current_sequence_id,
                prev_sequence_id=prev_sequence_id,
                action=action,
                validation_result=validation_result if self.sequence_validation_enabled else None
            )
        except Exception as e:
            print(f"消息解析错误: {e}")
            return None

    async def start(self):
        """启动WebSocket客户端"""
        self.is_running = True
        print(f"启动OKX WebSocket客户端，交易对: {self.symbol}")

        # 重置序列号状态
        self.last_sequence_id = -1
        self.last_prev_sequence_id = -1
        self.sequence_errors_count = 0

        while self.is_running:
            try:
                async with websockets.connect(self.url) as ws:
                    self.ws_connection = ws

                    # 订阅频道
                    sub_param = {"op": "subscribe", "args": self.channels}
                    await ws.send(json.dumps(sub_param))
                    print(f"已订阅频道: {self.channels}")

                    # 处理消息
                    async for message in ws:
                        if not self.is_running:
                            break

                        # 检查是否需要重新连接
                        if self.should_reconnect():
                            print("🔄 序列号错误过多，主动重新连接...")
                            break

                        print(f"收到消息: {message[:100]}...")
                        message_with_timestamp = self.get_timestamp() + message

                        # 解析并放入队列
                        orderbook_data = self.parse_websocket_message_to_dataclass(message_with_timestamp)
                        if orderbook_data:
                            await global_orderbook_queue.put(orderbook_data)
                            # print(f"✓ 数据已放入队列 | 时间: {orderbook_data.timestamp.strftime('%H:%M:%S.%f')[:-3]}")
                            # 定期打印统计信息
                            if self.stats['total_messages'] % 100 == 0 and self.sequence_validation_enabled:
                                self.print_sequence_stats()

            except websockets.exceptions.ConnectionClosed:
                print("WebSocket连接关闭，正在重连...")
                await asyncio.sleep(5)
            except Exception as e:
                print(f"WebSocket错误: {e}")
                await asyncio.sleep(5)
            finally:
                self.ws_connection = None

    def print_sequence_stats(self):
        """打印序列号统计信息"""
        stats = self.get_sequence_stats()
        print(f"\n=== 序列号统计 ===")
        print(f"总消息数: {stats['total_messages']}")
        print(f"有效消息: {stats['valid_messages']}")
        print(f"序列号跳跃: {stats['sequence_gaps']}")
        print(f"重置次数: {stats['resets_detected']}")
        print(f"当前序列号: {stats['last_sequence_id']}")
        print(f"错误计数: {stats['sequence_errors_count']}")
        print("================\n")

    async def reset_sequence_validation(self):
        """重置序列号验证状态"""
        self.last_sequence_id = -1
        self.last_prev_sequence_id = -1
        self.sequence_errors_count = 0
        self.stats = {
            'total_messages': 0,
            'valid_messages': 0,
            'sequence_gaps': 0,
            'resets_detected': 0
        }
        print("序列号验证状态已重置")

    async def stop(self):
        """停止WebSocket客户端"""
        self.is_running = False
        if self.ws_connection:
            await self.ws_connection.close()
        print("OKX WebSocket客户端已停止")

    async def unsubscribe(self):
        """取消订阅"""
        if self.ws_connection:
            unsub_param = {"op": "unsubscribe", "args": self.channels}
            await self.ws_connection.send(json.dumps(unsub_param))
            print("已取消订阅")

    # 保留原有的工具方法，但改为实例方法
    def partial(self, res):
        data_obj = res['data'][0]
        bids = data_obj['bids']
        asks = data_obj['asks']
        instrument_id = res['arg']['instId']
        return bids, asks, instrument_id

    # def update_bids(self, res, bids_p):
    #     # 原有的update_bids逻辑...
    #     bids_u = res['data'][0]['bids']
    #     for i in bids_u:
    #         bid_price = i[0]
    #         for j in bids_p:
    #             if bid_price == j[0]:
    #                 if i[1] == '0':
    #                     bids_p.remove(j)
    #                     break
    #                 else:
    #                     del j[1]
    #                     j.insert(1, i[1])
    #                     break
    #         else:
    #             if i[1] != "0":
    #                 bids_p.append(i)
    #     else:
    #         bids_p.sort(key=lambda price: self.sort_num(price[0]), reverse=True)
    #     return bids_p

    def update_bids(self, res, bids_p):
        # 获取增量bids数据
        bids_u = res['data'][0]['bids']
        # print('增量数据bids为：' + str(bids_u))
        # print('档数为：' + str(len(bids_u)))
        # bids合并
        for i in bids_u:
            bid_price = i[0]
            for j in bids_p:
                if bid_price == j[0]:
                    if i[1] == '0':
                        bids_p.remove(j)
                        break
                    else:
                        del j[1]
                        j.insert(1, i[1])
                        break
                else:
                    if i[1] != "0":
                        bids_p.append(i)

        bids_p.sort(key=lambda price: self.sort_num(price[0]), reverse=True)
        # print('合并后的bids为：' + str(bids_p) + '，档数为：' + str(len(bids_p)))
        return bids_p

    def update_asks(self, res, asks_p):
        # 获取增量asks数据
        asks_u = res['data'][0]['asks']
        # print('增量数据asks为：' + str(asks_u))
        # print('档数为：' + str(len(asks_u)))
        # asks合并
        for i in asks_u:
            ask_price = i[0]
            for j in asks_p:
                if ask_price == j[0]:
                    if i[1] == '0':
                        asks_p.remove(j)
                        break
                    else:
                        del j[1]
                        j.insert(1, i[1])
                        break
                else:
                    if i[1] != "0":
                        asks_p.append(i)

        asks_p.sort(key=lambda price: self.sort_num(price[0]))
        # print('合并后的asks为：' + str(asks_p) + '，档数为：' + str(len(asks_p)))
        return asks_p

    def sort_num(self, n):
        if isinstance(n, str) and n.isdigit():
            return int(n)
        else:
            return float(n)

    def check(self, bids, asks):
        # 获取bid档str
        bids_l = []
        bid_l = []
        count_bid = 1
        while count_bid <= 25:
            if count_bid > len(bids):
                break
            bids_l.append(bids[count_bid - 1])
            count_bid += 1
        for j in bids_l:
            str_bid = ':'.join(j[0: 2])
            bid_l.append(str_bid)
        # 获取ask档str
        asks_l = []
        ask_l = []
        count_ask = 1
        while count_ask <= 25:
            if count_ask > len(asks):
                break
            asks_l.append(asks[count_ask - 1])
            count_ask += 1
        for k in asks_l:
            str_ask = ':'.join(k[0: 2])
            ask_l.append(str_ask)
        # 拼接str
        num = ''
        if len(bid_l) == len(ask_l):
            for m in range(len(bid_l)):
                num += bid_l[m] + ':' + ask_l[m] + ':'
        elif len(bid_l) > len(ask_l):
            # bid档比ask档多
            for n in range(len(ask_l)):
                num += bid_l[n] + ':' + ask_l[n] + ':'
            for l in range(len(ask_l), len(bid_l)):
                num += bid_l[l] + ':'
        elif len(bid_l) < len(ask_l):
            # ask档比bid档多
            for n in range(len(bid_l)):
                num += bid_l[n] + ':' + ask_l[n] + ':'
            for l in range(len(bid_l), len(ask_l)):
                num += ask_l[l] + ':'

        new_num = num[:-1]
        int_checksum = zlib.crc32(new_num.encode())
        fina = self.change(int_checksum)
        return fina

    def change(self, num_old):
        num = pow(2, 31) - 1
        if num_old > num:
            out = num_old - num * 2 - 2
        else:
            out = num_old
        return out
