{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DerivingVia #-}

module Protocol.Types where

import Data.Binary
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BL
import Data.Int
import Data.Map qualified as M
import Data.String
import Data.Text qualified as T

data Assembly
  = SimpleMethod MethodPayload
  | Heartbeat
  | ContentMethod MethodPayload ContentHeaderProperties BL.ByteString -- method, properties, content-data
  deriving (Show)

data Frame = Frame ChannelID FramePayload -- channel, payload
  deriving (Show)

data FramePayload
  = MethodPayload MethodPayload
  | ContentHeaderPayload ShortInt ShortInt LongLongInt ContentHeaderProperties -- classID, weight, bodySize, propertyFields
  | ContentBodyPayload BL.ByteString
  | HeartbeatPayload
  deriving (Show)

type Timestamp = Word64

type Octet = Word8

type Bit = Bool

type ChannelID = ShortInt

type PayloadSize = LongInt

type ShortInt = Word16

type LongInt = Word32

type LongLongInt = Word64

data DecimalValue = DecimalValue Octet LongInt
  deriving (Eq, Ord, Read, Show)

newtype ShortString = ShortString {ss :: T.Text}
  deriving (Eq, Ord, Read, Show, IsString)

newtype LongString = LongString BS.ByteString
  deriving (Eq, Ord, Read, Show)

data FieldTable = FieldTable (M.Map T.Text FieldValue)
  deriving (Eq, Ord, Read, Show)

newtype QueueName = QueueName T.Text
  deriving (Eq, Ord, Read, Show, IsString)

newtype ExchangeName = ExchangeName T.Text
  deriving (Eq, Ord, Read, Show, IsString)

newtype ConsumerTag = ConsumerTag T.Text
  deriving (Eq, Ord, Read, Show, IsString)

data ExhangeType = Fanout | Direct | Topic | Headers
  deriving (Eq, Ord, Read, Show)

data FieldValue
  = FVBool Bool
  | FVInt8 Int8
  | FVInt16 Int16
  | FVInt32 Int32
  | FVInt64 Int64
  | FVFloat Float
  | FVDouble Double
  | FVDecimal DecimalValue
  | FVString BS.ByteString
  | FVFieldArray [FieldValue]
  | FVTimestamp Timestamp
  | FVFieldTable FieldTable
  | FVVoid
  | FVByteArray BS.ByteString
  deriving (Eq, Ord, Read, Show)

data MethodConnection
  = Connection_start
      Octet -- version-major
      Octet -- version-minor
      FieldTable -- server-properties
      LongString -- mechanisms
      LongString -- locales
  | Connection_start_ok
      FieldTable -- client-properties
      ShortString -- mechanism
      LongString -- response
      ShortString -- locale
  | Connection_secure {challange :: LongString}
  | Connection_secure_ok LongString -- response
  | Connection_tune {channel_max :: ShortInt, frame_max :: LongInt, heartbeat_interval :: ShortInt}
  | Connection_tune_ok {channel_max :: ShortInt, frame_max :: LongInt, heartbeat_interval :: ShortInt}
  | Connection_open
      ShortString -- virtual-host
      ShortString -- reserved-1
      Bit -- reserved-2
  | Connection_open_ok
      ShortString -- reserved-1
  | Connection_close
      ShortInt -- reply-code
      ShortString -- reply-text
      ShortInt -- class-id
      ShortInt -- method-id
  | Connection_close_ok
  | Connection_blocked
      ShortString -- reason
  | Connection_unblocked
  deriving (Show)

data MethodPayload
  = MethodConnection MethodConnection
  | Channel_open
      ShortString -- reserved-1
  | Channel_open_ok
      LongString -- reserved-1
  | Channel_flow
      Bit -- active
  | Channel_flow_ok
      Bit -- active
  | Channel_close
      ShortInt -- reply-code
      ShortString -- reply-text
      ShortInt -- class-id
      ShortInt -- method-id
  | Channel_close_ok
  | Exchange_declare
      ShortInt -- reserved-1
      ExchangeName -- exchange
      ExhangeType -- typ
      Bit -- passive
      Bit -- durable
      Bit -- auto-delete
      Bit -- internal
      Bit -- no-wait
      FieldTable -- arguments
  | Exchange_declare_ok
  | Exchange_delete
      ShortInt -- reserved-1
      ExchangeName -- exchange
      Bit -- if-unused
      Bit -- no-wait
  | Exchange_delete_ok
  | Exchange_bind
      ShortInt -- reserved-1
      ExchangeName -- destination
      ExchangeName -- source
      ShortString -- routing-key
      Bit -- no-wait
      FieldTable -- arguments
  | Exchange_bind_ok
  | Exchange_unbind
      ShortInt -- reserved-1
      ExchangeName -- destination
      ExchangeName -- source
      ShortString -- routing-key
      Bit -- no-wait
      FieldTable -- arguments
  | Exchange_unbind_ok
  | Queue_declare {qReserved :: ShortInt, qName :: QueueName, passive :: Bit, durable :: Bit, exclusive :: Bit, auto_delete :: Bit, no_wait :: Bit, args :: FieldTable}
  | Queue_declare_ok {qName :: QueueName, message_count :: LongInt, consumer_count :: LongInt}
  | Queue_bind {qReserved :: ShortInt, qName :: QueueName, exName :: ExchangeName, routing_key :: ShortString, no_wait :: Bit, args :: FieldTable}
  | Queue_bind_ok
  | Queue_unbind {qReserved :: ShortInt, qName :: QueueName, exName :: ExchangeName, routing_key :: ShortString, args :: FieldTable}
  | Queue_unbind_ok
  | Queue_purge {qReserved :: ShortInt, qName :: QueueName, no_wait :: Bit}
  | Queue_purge_ok {message_count :: LongInt}
  | Queue_delete {qReserved :: ShortInt, qName :: QueueName, delete_if_unused :: Bit, delete_if_empty :: Bit, no_wait :: Bit}
  | Queue_delete_ok {message_count :: LongInt}
  | Basic_qos
      LongInt -- prefetch-size
      ShortInt -- prefetch-count
      Bit -- global
  | Basic_qos_ok
  | Basic_consume
      ShortInt -- reserved-1
      QueueName -- queue
      ConsumerTag -- consumer-tag
      Bit -- no_local; If the no-local field is set the server will not send messages to the client that published them.
      Bit -- no-ack
      Bit -- exclusive; Request exclusive consumer access, meaning only this consumer can access the queue.
      Bit -- no-wait
      FieldTable -- arguments
  | Basic_consume_ok
      ConsumerTag -- consumer-tag
  | Basic_cancel
      ConsumerTag -- consumer-tag
      Bit -- no-wait
  | Basic_cancel_ok
      ConsumerTag -- consumer-tag
  | Basic_publish
      ShortInt -- reserved-1
      ExchangeName -- exchange
      ShortString -- routing-key
      Bit -- mandatory -- mandatory; if true, the server might return the msg, which is currently not handled
      Bit -- immediate -- immediate; not customizable, as it is currently not supported anymore by RabbitMQ
  | Basic_return
      ShortInt -- reply-code
      ShortString -- reply-text
      ExchangeName -- exchange
      ShortString -- routing-key
  | Basic_deliver
      ConsumerTag -- consumer-tag
      LongLongInt -- delivery-tag
      Bit -- redelivered
      ExchangeName -- exchange
      ShortString -- routing-key
  | Basic_get
      ShortInt -- reserved-1
      QueueName -- queue
      Bit -- no-ack
  | Basic_get_ok
      LongLongInt -- delivery-tag
      Bit -- redelivered
      ExchangeName -- exchange
      ShortString -- routing-key
      LongInt -- message-count
  | Basic_get_empty
      ShortString -- reserved-1
  | Basic_ack
      LongLongInt -- delivery-tag
      Bit -- multiple
  | Basic_reject
      LongLongInt -- delivery-tag
      Bit -- requeue
  | Basic_recover_async
      Bit -- requeue
  | Basic_recover
      Bit -- requeue
  | Basic_recover_ok
  | Basic_nack
      LongLongInt -- delivery-tag
      Bit -- multiple
      Bit -- requeue
  | Tx_select
  | Tx_select_ok
  | Tx_commit
  | Tx_commit_ok
  | Tx_rollback
  | Tx_rollback_ok
  | Confirm_select
      Bit -- nowait
  | Confirm_select_ok
  deriving (Show)

data ContentHeaderProperties
  = CHConnection
  | CHChannel
  | CHExchange
  | CHQueue
  | CHBasic
      { content_type :: (Maybe ShortString),
        content_encoding :: (Maybe ShortString),
        headers :: (Maybe FieldTable),
        delivery_mode :: (Maybe Octet),
        priority :: (Maybe Octet),
        correlation_id :: (Maybe ShortString),
        reply_to :: (Maybe ShortString),
        expiration :: (Maybe ShortString),
        message_id :: (Maybe ShortString),
        timestamp :: (Maybe Timestamp),
        message_type :: (Maybe ShortString),
        user_id :: (Maybe ShortString),
        application_id :: (Maybe ShortString),
        reserved :: (Maybe ShortString)
      }
  | CHTx
  | CHConfirm
  deriving (Show)
