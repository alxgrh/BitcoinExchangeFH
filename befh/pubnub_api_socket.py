from befh.api_socket import ApiSocket
from befh.util import Logger
import threading
import json
import time
import zlib

from pubnub.pnconfiguration import PNConfiguration, PNReconnectionPolicy
from pubnub.pubnub import PubNub
 
from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNOperationType, PNStatusCategory
 

class PubNubApiClient(ApiSocket):
    """
    WebSocket client for PubNub (https://www.pubnub.com/) service
    """

    def __init__(self, id, received_data_compressed=False):
        """
        Constructor
        :param id: Socket id
        """
        ApiSocket.__init__(self)
        pnconfig = PNConfiguration()
        pnconfig.subscribe_key = "sub-c-e12e9174-dd60-11e6-806b-02ee2ddab7fe"
        pnconfig.publish_key = "blank"
        pnconfig.ssl = True
        pnconfig.reconnect_policy = PNReconnectionPolicy.LINEAR

        self.pubnub = PubNub(pnconfig)
        self.message_processor = SubscribeCallback()
        self.on_message_handler = None
        self.id = id
        self.wst = None             # Web socket thread
        self._connecting = False
        self._connected = False

    def connect(self, 
                channels = None,
                on_message_handler=None,
                reconnect_interval=10):
        """
        :param on_message_handler: Message handler which take the message as
                           the first argument
        """
        Logger.info(self.__class__.__name__, "Connecting to socket <%s>..." % self.id)
        if on_message_handler is not None:
            self.on_message_handler = on_message_handler
            self.message_processor.message = self.__on_message
            self.message_processor.status = self.__status
            self.pubnub.add_listener(self.message_processor)

            self.wst = threading.Thread(target=lambda: self.__start(channels,reconnect_interval=reconnect_interval))
            self.wst.start()

            return self.wst


    def __start(self, channels, reconnect_interval=10):
        self.pubnub.subscribe().channels(channels).execute()
        #while True:
        #    self.pubnub.subscribe().channels(channels).execute()
        #    Logger.info(self.__class__.__name__, "Socket <%s> is going to reconnect..." % self.id)
        #    time.sleep(reconnect_interval)

    def __on_message(self, pubnub, message):
        self.on_message_handler(message)

    def __status (self, pubnub, status):
        if status.operation == PNOperationType.PNSubscribeOperation \
                or status.operation == PNOperationType.PNUnsubscribeOperation:
            if status.category == PNStatusCategory.PNConnectedCategory:
                # This is expected for a subscribe, this means there is no error or issue whatsoever
                Logger.info(self.__class__.__name__, "Socket <%s> is opened." % self.id)
                self._connected = True
            elif status.category == PNStatusCategory.PNReconnectedCategory:
                Logger.info(self.__class__.__name__, "Socket <%s> is reconnected." % self.id)
                self._connected = True
                # This usually occurs if subscribe temporarily fails but reconnects. This means
                # there was an error but there is no longer any issue
            elif status.category == PNStatusCategory.PNDisconnectedCategory:
                # This is the expected category for an unsubscribe. This means there
                # was no error in unsubscribing from everything
                Logger.info(self.__class__.__name__, "Socket <%s> is closed." % self.id)
                self._connecting = False
                self._connected = False
            elif status.category == PNStatusCategory.PNUnexpectedDisconnectCategory:
                # This is usually an issue with the internet connection, this is an error, handle
                # appropriately retry will be called automatically
                Logger.info(self.__class__.__name__, "Socket <%s> is closed unexpectedly." % self.id)
                self._connecting = False
                self._connected = False
            elif status.category == PNStatusCategory.PNAccessDeniedCategory:
                pass
                # This means that PAM does allow this client to subscribe to this
                # channel and channel group configuration. This is another explicit error
            else:
                pass
                # This is usually an issue with the internet connection, this is an error, handle appropriately
                # retry will be called automatically
        elif status.operation == PNOperationType.PNSubscribeOperation:
            # Heartbeat operations can in fact have errors, so it is important to check first for an error.
            # For more information on how to configure heartbeat notifications through the status
            # PNObjectEventListener callback, consult <link to the PNCONFIGURATION heartbeart config>
            if status.is_error():
                # There was an error with the heartbeat operation, handle here
                Logger.info(self.__class__.__name__, "Socket <%s> error:\n %s" % (self.id, error))
            else:
                pass
                # Heartbeat operation was successful
        else:
            pass
            # Encountered unknown status type

