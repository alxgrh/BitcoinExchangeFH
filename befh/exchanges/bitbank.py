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
        pass
    @classmethod
    def parse_trade(cls, instmt, raw):
        """
        :param instmt: Instrument
        :param raw: Raw data in JSON
        :return:
        """
        pass

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

        Logger.info(self.__class__.__name__, message.message)
        return


    def start(self, instmt):
        """
        Start the exchange gateway
        :param instmt: Instrument
        :return List of threads
        """
        instmt.set_l2_depth(L2Depth(25))
        instmt.set_prev_l2_depth(L2Depth(25))
        instmt.set_instmt_snapshot_table_name(self.get_instmt_snapshot_table_name(instmt.get_exchange_name(),
                                                                                  instmt.get_instmt_name()))
        self.init_instmt_snapshot_table(instmt)
        channels = [self.api_socket.get_order_book_channel_name(instmt),self.api_socket.get_trades_channel_name(instmt)]
        return [self.api_socket.connect(channels, on_message_handler=partial(self.on_message_handler, instmt)
                                        )]

