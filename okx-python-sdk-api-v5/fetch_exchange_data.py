import base64
# import datetime
from datetime import datetime
import hmac
import json
import time
import zlib

import requests
import websockets
from data_queue import OrderBookData, global_orderbook_queue
import asyncio

import asyncio
from OKXWebSocketClient import OKXWebSocketClient


async def main():
    """主函数 - 仅当直接运行时执行"""
    client = OKXWebSocketClient(
        api_key="e85e1598-5cca-4212-97b2-dbda96200c93",
        secret_key="5C44AE07D08C0A589037D29E10008467",
        passphrase="Hans428571",
        symbol="SOL-USDT"
    )

    try:
        await client.start()
    except KeyboardInterrupt:
        print("\n收到中断信号...")
    finally:
        await client.stop()


if __name__ == "__main__":
    # 只有直接运行这个文件时才执行
    asyncio.run(main())