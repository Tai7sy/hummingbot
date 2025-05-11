import asyncio
import json
import random
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.gate_io import gate_io_constants as CONSTANTS, gate_io_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.gate_io.gate_io_exchange import GateIoExchange


class GateIoAPIOrderBookDataSource(OrderBookTrackerDataSource):

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'GateIoExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._trading_pairs: List[str] = trading_pairs

        self._message_queue: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        if web_utils.is_hidden_pair(trading_pair):
            return await self._request_order_book_snapshot_web(trading_pair)
        else:
            return await self._request_order_book_snapshot(trading_pair)

    async def _request_order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        params = {
            "currency_pair": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
            "with_id": json.dumps(True)
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        snapshot_response: Dict[str, Any] = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(endpoint=CONSTANTS.ORDER_BOOK_PATH_URL),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ORDER_BOOK_PATH_URL,
        )

        """
        {
            "id": 23559187965,
            "current": 1746968695686,
            "update": 1746968695634,
            "asks": [
                [
                    "104624.8",
                    "0.20416"
                ],
                [
                    "104627.9",
                    "0.005"
                ]
            ],
            "bids": [
                [
                    "104624.7",
                    "0.31423"
                ],
                [
                    "104624.6",
                    "0.17289"
                ]
            ]
        }
        """

        snapshot_timestamp: float = self._time()
        return OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": trading_pair,
                "update_id": snapshot_response["id"],
                "bids": snapshot_response["bids"],
                "asks": snapshot_response["asks"],
            },
            timestamp=snapshot_timestamp)

    async def _request_order_book_snapshot_web(self, trading_pair: str) -> OrderBookMessage:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.

        :param trading_pair: the trading pair for which the order book will be retrieved

        :return: the response from the exchange (JSON dictionary)
        """
        params = {
            "currency_pair": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
            # "limit": 50,
            # "interval": "0.0001"
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        snapshot_response: Dict[str, Any] = await rest_assistant.execute_request(
            url="https://www.gate.io/apiw/v2/spot/order_book",
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ORDER_BOOK_PATH_URL,
        )

        """
        {
        "timestamp": 1746968591255,
        "method": "/apiw/v2/spot/order_book",
        "code": 200,
        "message": "Success",
        "page": null,
        "limit": null,
        "total": null,
        "data": {
            "current_price": "328.63",
            "current": 1746968591249,
            "update": 1746968590558,
            "asks": [
                {
                    "p": "333.0000",
                    "s": "5.024"
                },
                {
                    "p": "333.4400",
                    "s": "0.236"
                }
            ],
             "bids": [
                {
                    "p": "328.6500",
                    "s": "2.565"
                },
                {
                    "p": "328.6300",
                    "s": "12.0214"
                }
            ]
            }
        }
        """
        snapshot_timestamp: float = self._time()
        return OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": trading_pair,
                "update_id": snapshot_response["data"]["current"],
                "bids": [[float(bid["p"]), float(bid["s"])] for bid in snapshot_response["data"]["bids"]],
                "asks": [[float(ask["p"]), float(ask["s"])] for ask in snapshot_response["data"]["asks"]],
            },
            timestamp=snapshot_timestamp)

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):

        if raw_message.get("method") == "trades.update":
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message["params"][0])
            for trade_data in raw_message["params"][1]:
                trade_timestamp: float = float(trade_data["time"])
                message_content = {
                    "trading_pair": trading_pair,
                    "trade_type": (float(TradeType.SELL.value)
                                   if trade_data["type"] == "sell"
                                   else float(TradeType.BUY.value)),
                    "trade_id": trade_data["id"],
                    "update_id": trade_timestamp,
                    "price": trade_data["price"],
                    "amount": trade_data["amount"],
                }
                trade_message: Optional[OrderBookMessage] = OrderBookMessage(
                    message_type=OrderBookMessageType.TRADE,
                    content=message_content,
                    timestamp=trade_timestamp)

                message_queue.put_nowait(trade_message)
        elif "result" in raw_message:
            trade_data: Dict[str, Any] = raw_message["result"]
            trade_timestamp: float = float(trade_data["create_time_ms"]) * 1e-3
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=trade_data["currency_pair"])
            message_content = {
                "trading_pair": trading_pair,
                "trade_type": (float(TradeType.SELL.value)
                               if trade_data["side"] == "sell"
                               else float(TradeType.BUY.value)),
                "trade_id": trade_data["id"],
                "update_id": trade_timestamp,
                "price": trade_data["price"],
                "amount": trade_data["amount"],
            }
            trade_message: Optional[OrderBookMessage] = OrderBookMessage(
                message_type=OrderBookMessageType.TRADE,
                content=message_content,
                timestamp=trade_timestamp)

            message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):

        # v3 api
        if raw_message.get("method") == "depth.update":
            diff_data: [str, Any] = raw_message["params"][1]
            timestamp: float = (diff_data["current"])
            update_id: int = diff_data["id"]

            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message["params"][2])
            order_book_message_content = {
                "trading_pair": trading_pair,
                "update_id": update_id,
                "first_update_id": update_id,
                "bids": diff_data["bids"],
                "asks": diff_data["asks"],
            }
            diff_message: OrderBookMessage = OrderBookMessage(
                OrderBookMessageType.DIFF,
                order_book_message_content,
                timestamp)

            message_queue.put_nowait(diff_message)
        elif "result" in raw_message:
            diff_data: [str, Any] = raw_message["result"]
            timestamp: float = (diff_data["t"]) * 1e-3
            update_id: int = diff_data["u"]

            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=diff_data["s"])

            order_book_message_content = {
                "trading_pair": trading_pair,
                "update_id": update_id,
                "first_update_id": diff_data["U"],
                "bids": diff_data["b"],
                "asks": diff_data["a"],
            }
            diff_message: OrderBookMessage = OrderBookMessage(
                OrderBookMessageType.DIFF,
                order_book_message_content,
                timestamp)

            message_queue.put_nowait(diff_message)

    async def _subscribe_channels(self, ws: WSAssistant):
        pass

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:

        # v3 api
        if event_message.get("method") == "depth.update":
            return self._diff_messages_queue_key
        if event_message.get("method") == "trades.update":
            return self._trade_messages_queue_key

        channel = ""
        if event_message.get("error") is not None:
            err_msg = event_message.get("error", {}).get("message", event_message.get("error"))
            raise IOError(f"Error event received from the server ({err_msg})")
        elif event_message.get("event") == "update":
            if event_message.get("channel") == CONSTANTS.ORDERS_UPDATE_ENDPOINT_NAME:
                channel = self._diff_messages_queue_key
            elif event_message.get("channel") == CONSTANTS.TRADES_ENDPOINT_NAME:
                channel = self._trade_messages_queue_key

        return channel

    async def _connected_websocket_assistant(self) -> WSAssistant:
        pass

    async def _connected_websocket_assistant_for_pair(self, trading_pair: str) -> WSAssistant:

        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        if web_utils.is_hidden_pair(trading_pair):
            ws: WSAssistant = await self._api_factory.get_ws_assistant()
            await ws.connect(ws_url="wss://webws.gateio.live/v3", ping_timeout=CONSTANTS.PING_TIMEOUT)

            """{id: 1648415, method: "trades.subscribe", params: ["XMR_USDT"]}"""
            trades_payload = {
                "id": random.randint(1000000, 9999999),  # random id
                "method": "trades.subscribe",
                "params": [symbol]
            }
            subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=trades_payload)

            """{id: 3621165, method: "depth.subscribe", params: ["XMR_USDT", 30, "0.01"]}"""
            order_book_payload = {
                "id": random.randint(1000000, 9999999), # random id
                "method": "depth.subscribe",
                "params": [symbol, 30, "0.01"] # limit = 30
            }
            subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=order_book_payload)

            await ws.send(subscribe_trade_request)
            await ws.send(subscribe_orderbook_request)

            return ws
        else:
            ws: WSAssistant = await self._api_factory.get_ws_assistant()
            await ws.connect(ws_url=CONSTANTS.WS_URL, ping_timeout=CONSTANTS.PING_TIMEOUT)

            trades_payload = {
                "time": int(self._time()),
                "channel": CONSTANTS.TRADES_ENDPOINT_NAME,
                "event": "subscribe",
                "payload": [symbol]
            }
            subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=trades_payload)

            order_book_payload = {
                "time": int(self._time()),
                "channel": CONSTANTS.ORDERS_UPDATE_ENDPOINT_NAME,
                "event": "subscribe",
                "payload": [symbol, "100ms"]
            }
            subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=order_book_payload)

            await ws.send(subscribe_trade_request)
            await ws.send(subscribe_orderbook_request)

            return ws

    async def listen_for_subscriptions(self):
        """
        Connects to the trade events and order diffs websocket endpoints and listens to the messages sent by the
        exchange. Each message is stored in its own queue.
        """

        async def handle_subscription(trading_pair):
            ws: Optional[WSAssistant] = None
            while True:
                try:
                    ws: WSAssistant = await self._connected_websocket_assistant_for_pair(trading_pair=trading_pair)
                    await self._subscribe_channels(ws)
                    await self._process_websocket_messages(websocket_assistant=ws)
                except asyncio.CancelledError:
                    raise
                except ConnectionError as connection_exception:
                    self.logger().warning(
                        f"The websocket connection to {trading_pair} was closed ({connection_exception})")
                except Exception:
                    self.logger().exception(
                        "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...",
                    )
                    await self._sleep(1.0)
                finally:
                    await self._on_order_stream_interruption(websocket_assistant=ws)

        tasks = [handle_subscription(trading_pair) for trading_pair in self._trading_pairs]
        await safe_gather(*tasks)
