from befh.market_data import L2Depth, Trade
from befh.exchanges.gateway import ExchangeGateway
from befh.instrument import Instrument
from befh.pubnub_api_socket import PubNubApiClient
from befh.util import Logger
import time
import threading
import json
from functools import partial
from datetime import datetime


class ExchGwBitbankPN(PubNubApiClient):
    """
    Exchange gateway Bitbank Websocket API via  PubNub (https://www.pubnub.com/) 
    """
    def __init__(self):
        """
        Constructor
        """
        PubNubApiClient.__init__(self, 'ExchGwBitbank')

    @classmethod
    def get_trades_timestamp_field_name(cls):
        return 'executed_at'

    @classmethod
    def get_bids_field_name(cls):
        return 'bids'

    @classmethod
    def get_asks_field_name(cls):
        return 'asks'

    @classmethod
    def get_trade_side_field_name(cls):
        return 'side'

    @classmethod
    def get_trade_id_field_name(cls):
        return 'transaction_id'

    @classmethod
    def get_trade_price_field_name(cls):
        return 'price'

    @classmethod
    def get_trade_volume_field_name(cls):
        return 'amount'
    
    @classmethod
    def get_order_book_channel_name(cls, instmt):
        return 'depth_'+instmt.get_instmt_code()

    @classmethod
    def get_trades_channel_name(cls, instmt):
        return 'transactions_'+instmt.get_instmt_code()

    @classmethod
    def parse_l2_depth(cls, instmt, raw):
        """
        Parse raw data to L2 depth
        :param instmt: Instrument
        :param raw: Raw data in JSON
        """
        l2_depth = instmt.get_l2_depth()
        keys = list(raw.keys())
        if cls.get_bids_field_name() in keys and \
           cls.get_asks_field_name() in keys:

            l2_depth.date_time = datetime.utcnow().strftime("%Y%m%d %H:%M:%S.%f")

            bids = raw[cls.get_bids_field_name()]
            bids_len = min(l2_depth.depth, len(bids))
            for i in range(0, bids_len):
                l2_depth.bids[i].price = float(bids[i][0]) if not isinstance(bids[i][0], float) else bids[i][0]
                l2_depth.bids[i].volume = float(bids[i][1]) if not isinstance(bids[i][1], float) else bids[i][1]

            asks = raw[cls.get_asks_field_name()]
            asks_len = min(l2_depth.depth, len(asks))
            for i in range(0, asks_len):
                l2_depth.asks[i].price = float(asks[i][0]) if not isinstance(asks[i][0], float) else asks[i][0]
                l2_depth.asks[i].volume = float(asks[i][1]) if not isinstance(asks[i][1], float) else asks[i][1]
        else:
            raise Exception('Does not contain order book keys in instmt %s-%s.\nOriginal:\n%s' % \
                (instmt.get_exchange_name(), instmt.get_instmt_name(), \
                 raw))

        return l2_depth
    
    @classmethod
    def parse_trade(cls, instmt, raw):
        """
        :param instmt: Instrument
        :param raw: Raw data in JSON
        :return:
        """

        trade = Trade()
        keys = list(raw.keys())

        if cls.get_trades_timestamp_field_name() in keys and \
           cls.get_trade_id_field_name() in keys and \
           cls.get_trade_side_field_name() in keys and \
           cls.get_trade_price_field_name() in keys and \
           cls.get_trade_volume_field_name() in keys:
            
            trade = Trade()
            trade_id = raw[cls.get_trade_id_field_name()]
            timestamp = raw[cls.get_trades_timestamp_field_name()]
            trade_price = raw[cls.get_trade_price_field_name()]
            trade_volume = raw[cls.get_trade_volume_field_name()]
    
            trade.date_time = datetime.utcfromtimestamp(timestamp/1000).strftime("%Y%m%d %H:%M:%S.%f")
            trade.trade_side = Trade.parse_side(raw[cls.get_trade_side_field_name()])
            trade.trade_volume = float(trade_volume)
            trade.trade_id = str(trade_id)
            trade.trade_price = float(trade_price)
        return trade


class ExchGwBitbank(ExchangeGateway):
    """
    Exchange gateway Bitbank
    """
    def __init__(self, db_clients):
        """
        Constructor
        :param db_client: Database client
        """
        ExchangeGateway.__init__(self, ExchGwBitbankPN(), db_clients)

    @classmethod
    def get_exchange_name(cls):
        """
        Get exchange name
        :return: Exchange name string
        """
        return 'Bitbank'

    def on_open_handler(self, instmt, ws):
        """
        Socket on open handler
        :param instmt: Instrument
        :param ws: Web socket
        """
        Logger.info(self.__class__.__name__, "Instrument %s is subscribed in channel %s" % \
                  (instmt.get_instmt_code(), instmt.get_exchange_name()))
        if not instmt.get_subscribed():
            instmt.set_subscribed(True)

    def on_close_handler(self, instmt, ws):
        """
        Socket on close handler
        :param instmt: Instrument
        :param ws: Web socket
        """
        Logger.info(self.__class__.__name__, "Instrument %s is unsubscribed in channel %s" % \
                  (instmt.get_instmt_code(), instmt.get_exchange_name()))
        instmt.set_subscribed(False)

    def on_message_handler(self, instmt, message):
        """
        Incoming message handler
        :param instmt: Instrument
        :param message: Message
        """
        if isinstance(message.message, dict):
            msg = message.message
            if 'asks' in msg['data']:
                instmt.set_prev_l2_depth(instmt.get_l2_depth().copy())
                self.api_socket.parse_l2_depth(instmt, msg['data'])

                if instmt.get_l2_depth().is_diff(instmt.get_prev_l2_depth()):
                    instmt.incr_order_book_id()
                    self.insert_order_book(instmt)

            elif 'transactions' in msg['data']:
                for transaction in msg['data']['transactions']:
                    trade = self.api_socket.parse_trade(instmt, transaction)
                    if int(trade.trade_id) > int(instmt.get_exch_trade_id()):
                        instmt.incr_trade_id()
                        instmt.set_exch_trade_id(trade.trade_id)
                        self.insert_trade(instmt, trade)
            else:
                pass

        #Logger.info(self.__class__.__name__, message.message)
        return


    def start(self, instmt):
        """
        Start the exchange gateway
        :param instmt: Instrument
        :return List of threads
        """
        instmt.set_l2_depth(L2Depth(30))
        instmt.set_prev_l2_depth(L2Depth(30))
        instmt.set_instmt_snapshot_table_name(self.get_instmt_snapshot_table_name(instmt.get_exchange_name(),
                                                                                  instmt.get_instmt_name()))
        self.init_instmt_snapshot_table(instmt)
        channels = [self.api_socket.get_order_book_channel_name(instmt),self.api_socket.get_trades_channel_name(instmt)]
        return [self.api_socket.connect(channels, on_message_handler=partial(self.on_message_handler, instmt)
                                        )]

