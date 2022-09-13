{-# LANGUAGE RecordWildCards #-}

module Api where

import Control.Exception as E
import Data.ByteString.Lazy qualified as BL
import Data.Map qualified as M
import Data.Text qualified as T
import Protocol.Types
import Util

data Mechanism = Plain T.Text T.Text

data ConnectionOpts = ConnectionOpts
  { optServer :: (String, String),
    optVHost :: !T.Text,
    optNamePass :: (T.Text, T.Text),
    optMaxFrameSize :: !(Maybe Word32),
    optHeartbeatDelay :: !(Maybe Word16),
    optMaxChannel :: !(Maybe Word16)
  }

defaultConnectionOpts :: ConnectionOpts
defaultConnectionOpts = ConnectionOpts ("localhost", "5672") "/" ("guest", "guest") (Just 131072) Nothing Nothing

data RabbitMQException
  = ChannelClosedByServer T.Text
  | ChannelClosedByUser
  | ProtocolError
  | ConnectionClosedByUser
  | ConnectionClosedByServer
  | ConnectionMissingHeartbeat
  | MaxChannelReached
  | NetworkError
  deriving (Generic, Show, Eq)

instance E.Exception RabbitMQException

type Ack = Bool

data PublishError = PublishError
  { errReplyCode :: ReturnReplyCode,
    errExchange :: Maybe ExchangeName,
    errRoutingKey :: T.Text
  }
  deriving (Eq, Read, Show)

data ReturnReplyCode
  = Unroutable T.Text
  | NoConsumers T.Text
  | NotFound T.Text
  deriving (Eq, Read, Show)

createPublishError :: MethodPayload -> PublishError
createPublishError (Basic_return code (ShortString errText) (exchange) (ShortString routingKey)) =
  let replyError = case code of
        312 -> Unroutable errText
        313 -> NoConsumers errText
        404 -> NotFound errText
        num -> error $ "unexpected return error code: " ++ show num
      pubError = PublishError replyError (Just exchange) routingKey
   in pubError
createPublishError x = error $ "basicReturnToPublishError fail: " ++ show x

data DeliveryMode
  = Persistent --   the message will survive server restarts (if the queue is durable)
  | NonPersistent --   the message may be lost after server restarts
  deriving (Eq, Ord, Read, Show)

deliveryModeToInt :: DeliveryMode -> Octet
deliveryModeToInt NonPersistent = 1
deliveryModeToInt Persistent = 2

intToDeliveryMode :: Octet -> DeliveryMode
intToDeliveryMode 1 = NonPersistent
intToDeliveryMode 2 = Persistent
intToDeliveryMode n = error ("Unknown delivery mode int: " ++ show n)

data Message = Message
  { -- | the content of your message
    msgBody :: BL.ByteString,
    -- | see 'DeliveryMode'
    msgDeliveryMode :: Maybe DeliveryMode,
    -- | use in any way you like; this doesn't affect the way the message is handled
    msgTimestamp :: Maybe Timestamp,
    -- | use in any way you like; this doesn't affect the way the message is handled
    msgID :: Maybe ShortString,
    -- | use in any way you like; this doesn't affect the way the message is handled
    msgType :: Maybe ShortString,
    msgUserID :: Maybe ShortString,
    msgApplicationID :: Maybe ShortString,
    msgClusterID :: Maybe ShortString,
    msgContentType :: Maybe ShortString,
    msgContentEncoding :: Maybe ShortString,
    msgReplyTo :: Maybe ShortString,
    msgPriority :: Maybe Octet,
    msgCorrelationID :: Maybe ShortString,
    msgExpiration :: Maybe ShortString,
    msgHeaders :: Maybe FieldTable
  }
  deriving (Eq, Ord, Read, Show)

newMsg :: Message
newMsg = Message BL.empty Nothing Nothing Nothing Nothing Nothing Nothing Nothing Nothing Nothing Nothing Nothing Nothing Nothing Nothing

createMessage :: ContentHeaderProperties -> BL.ByteString -> Message
createMessage (CHBasic {..}) body =
  Message body (fmap intToDeliveryMode delivery_mode) timestamp message_id message_type user_id application_id reserved content_type content_encoding reply_to priority correlation_id (expiration) headers
createMessage c _ = error ("Unknown content header properties: " ++ show c)

-- | contains meta-information of a delivered message (through 'getMsg' or 'consumeMsgs')
data Envelope = Envelope
  { envDeliveryTag :: LongLongInt,
    envRedelivered :: Bool,
    envExchangeName :: ExchangeName,
    envRoutingKey :: T.Text
  }
  deriving (Show)

data QueueOpts = QueueOpts
  { -- | (default \"\"); the name of the queue; if left empty, the server will generate a new name and return it from the 'declareQueue' method
    queueName :: QueueName,
    -- | (default 'False'); If set, the server will not create the queue.  The client can use this to check whether a queue exists without modifying the server state.
    queuePassive :: Bool,
    -- | (default 'True'); If set when creating a new queue, the queue will be marked as durable. Durable queues remain active when a server restarts. Non-durable queues (transient queues) are purged if/when a server restarts. Note that durable queues do not necessarily hold persistent messages, although it does not make sense to send persistent messages to a transient queue.
    queueDurable :: Bool,
    -- | (default 'False'); Exclusive queues may only be consumed from by the current connection. Setting the 'exclusive' flag always implies 'auto-delete'.
    queueExclusive :: Bool,
    -- | (default 'False'); If set, the queue is deleted when all consumers have finished using it. Last consumer can be cancelled either explicitly or because its channel is closed. If there was no consumer ever on the queue, it won't be deleted.
    queueAutoDelete :: Bool,
    -- | (default empty): Headers to use when creating this queue, such as @x-message-ttl@ or @x-dead-letter-exchange@.
    queueHeaders :: FieldTable
  }
  deriving (Eq, Ord, Read, Show)

newQueue :: QueueOpts
newQueue = QueueOpts "" False True False False (FieldTable M.empty)

data ExchangeOpts = ExchangeOpts
  { -- | (must be set); the name of the exchange
    exchangeName :: ExchangeName,
    -- | (must be set); the type of the exchange (\"fanout\", \"direct\", \"topic\", \"headers\")
    exchangeType :: ExhangeType,
    -- | (default 'False'); If set, the server will not create the exchange. The client can use this to check whether an exchange exists without modifying the server state.
    exchangePassive :: Bool,
    -- | (default 'True'); If set when creating a new exchange, the exchange will be marked as durable. Durable exchanges remain active when a server restarts. Non-durable exchanges (transient exchanges) are purged if/when a server restarts.
    exchangeDurable :: Bool,
    -- | (default 'False'); If set, the exchange is deleted when all queues have finished using it.
    exchangeAutoDelete :: Bool,
    -- | (default 'False'); If set, the exchange may not be used directly by publishers, but only when bound to other exchanges. Internal exchanges are used to construct wiring that is not visible to applications.
    exchangeInternal :: Bool,
    -- | (default empty); A set of arguments for the declaration. The syntax and semantics of these arguments depends on the server implementation.
    exchangeArguments :: FieldTable
  }
  deriving (Eq, Ord, Read, Show)

-- | an 'ExchangeOpts' with defaults set; you must override at least the 'exchangeName' and 'exchangeType' fields.
newExchange :: ExchangeOpts
newExchange = ExchangeOpts "" Direct False True False False (FieldTable M.empty)

emptyArgs :: FieldTable
emptyArgs = FieldTable M.empty
