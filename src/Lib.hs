{-# LANGUAGE RecordWildCards #-}

module Lib (module Lib, module Api, Channel (chid), Connection) where

import Api
import Channel
import Connection
import Data.Map qualified as M
import Data.Text qualified as T
import Protocol.Types
import Util

declareQueue :: Channel -> QueueOpts -> IO (QueueName, Int, Int)
declareQueue chan QueueOpts {..} = do
  SimpleMethod (Queue_declare_ok qName messageCount consumerCount) <- request chan $ msg
  return (qName, fromIntegral messageCount, fromIntegral consumerCount)
  where
    msg = SimpleMethod $ Queue_declare 1 (queueName) (queuePassive) (queueDurable) (queueExclusive) (queueAutoDelete) False (queueHeaders)

bindQueue :: Channel -> QueueName -> ExchangeName -> ShortString -> FieldTable -> IO ()
bindQueue chan queue exchange routingKey args = do
  (SimpleMethod Queue_bind_ok) <- request chan msg
  return ()
  where
    msg = SimpleMethod $ Queue_bind 1 queue exchange (routingKey) False args

unbindQueue :: Channel -> QueueName -> ExchangeName -> ShortString -> FieldTable -> IO ()
unbindQueue chan queue exchange routingKey args = do
  (SimpleMethod Queue_unbind_ok) <- request chan msg
  return ()
  where
    msg = SimpleMethod $ Queue_unbind 1 queue exchange routingKey args

purgeQueue :: Channel -> QueueName -> IO Word32
purgeQueue chan queue = do
  SimpleMethod (Queue_purge_ok msgCount) <- request chan $ SimpleMethod $ Queue_purge 1 queue False
  return msgCount

-- | deletes the queue; returns the number of messages that were in the queue before deletion
deleteQueue :: Channel -> QueueName -> IO Word32
deleteQueue chan queue = do
  SimpleMethod (Queue_delete_ok msgCount) <- request chan $ SimpleMethod $ Queue_delete 1 queue False False False
  return msgCount

----------------------------------

declareExchange :: Channel -> ExchangeOpts -> IO ()
declareExchange chan (ExchangeOpts {..}) = do
  (SimpleMethod Exchange_declare_ok) <- request chan msg
  return ()
  where
    msg = SimpleMethod $ Exchange_declare 1 exchangeName exchangeType exchangePassive exchangeDurable exchangeAutoDelete exchangeInternal False exchangeArguments

bindExchange :: Channel -> ExchangeName -> ExchangeName -> T.Text -> FieldTable -> IO ()
bindExchange chan destinationName sourceName routingKey args = do
  (SimpleMethod Exchange_bind_ok) <- request chan (SimpleMethod (Exchange_bind 1 (destinationName) (sourceName) (ShortString routingKey) False args))
  return ()

unbindExchange :: Channel -> ExchangeName -> ExchangeName -> T.Text -> FieldTable -> IO ()
unbindExchange chan destinationName sourceName routingKey args = do
  SimpleMethod Exchange_unbind_ok <- request chan $ SimpleMethod $ Exchange_unbind 1 (destinationName) (sourceName) (ShortString routingKey) False args
  return ()

--
---- | deletes the exchange with the provided name
deleteExchange :: Channel -> ExchangeName -> IO ()
deleteExchange chan exchange = do
  (SimpleMethod Exchange_delete_ok) <- request chan (SimpleMethod (Exchange_delete 1 (exchange) False False))
  return ()

---------------------

consumeMsgs :: Channel -> QueueName -> Ack -> Callback -> IO ConsumerTag
consumeMsgs chan queue autoack callback =
  consumeMsgs' chan queue autoack callback (return ()) (FieldTable M.empty)

consumeMsgs' :: Channel -> QueueName -> Ack -> (Callback) -> IO () -> FieldTable -> IO ConsumerTag
consumeMsgs' chan queue autoack callback cancelCB args = atomically $ do
  tag <- addMessageListener chan (callback, cancelCB)
  requestAsync chan (msg tag)
  return tag
  where
    msg tag = SimpleMethod $ Basic_consume 1 (queue) (tag) False autoack False True args

publishMsg :: Channel -> ExchangeName -> T.Text -> Message -> IO (Maybe Int)
publishMsg chan exchange routingKey msg = publishMsg' chan exchange routingKey False msg

-- | Like 'publishMsg', but additionally allows you to specify whether the 'mandatory' flag should be set.
publishMsg' :: Channel -> ExchangeName -> T.Text -> Bool -> Message -> IO (Maybe Int)
publishMsg' chan exchange routingKey mandatory (Message {..}) = atomically $ do
  seqNumber <- confirmAdd (chan)
  requestAsync chan $
    ContentMethod
      (Basic_publish 1 (exchange) (ShortString routingKey) mandatory False)
      (CHBasic (msgContentType) (msgContentEncoding) (msgHeaders) (deliveryModeToInt <$> msgDeliveryMode) (msgPriority) (msgCorrelationID) (msgReplyTo) (msgExpiration) (msgID) (msgTimestamp) (msgType) (msgUserID) (msgApplicationID) (msgClusterID))
      (msgBody)
  return seqNumber

ackMsg :: Channel -> LongLongInt -> Bool -> IO ()
ackMsg chan deliveryTag multiple = atomically $ requestAsync chan $ SimpleMethod $ Basic_ack deliveryTag multiple

rejectMsg :: Channel -> LongLongInt -> Bool -> IO ()
rejectMsg chan deliveryTag requeue = atomically $ requestAsync chan $ SimpleMethod $ Basic_reject deliveryTag requeue

ack :: Channel -> Envelope -> IO ()
ack chan env = ackMsg chan (envDeliveryTag env) (False)

reject :: Channel -> Envelope -> Bool -> IO ()
reject chan env requeue = rejectMsg chan (envDeliveryTag env) requeue

---

getMsg :: Channel -> Ack -> QueueName -> IO (Maybe (Message, Envelope))
getMsg chan autoAck queue = do
  ret <- request chan $ SimpleMethod $ Basic_get 1 (queue) (autoAck)
  case ret of
    ContentMethod (Basic_get_ok deliveryTag redelivered exchange (ShortString routingKey) _) properties body ->
      return $ Just (createMessage properties body, Envelope deliveryTag redelivered exchange routingKey)
    _ -> return Nothing

----

confirmSelect :: Channel -> Bool -> IO ()
confirmSelect chan enable = atomically $ do
  confirmEnable (chan)
  requestAsync chan $ SimpleMethod (Confirm_select enable)

waitForConfirms :: Channel -> IO ()
waitForConfirms ch = atomically $ confirmWaitAll (ch)

addConfirmationListener :: Channel -> (ConfirmCallback) -> IO ()
addConfirmationListener chan = atomically . confirmAddListener (confirmation chan)

addReturnListener :: Channel -> ReturnCallback -> IO ()
addReturnListener (Channel {..}) cb = atomically $ addListener returnListeners cb

----
qos :: Channel -> Int -> Bool -> IO ()
qos chan prefetchCount global = do
  SimpleMethod Basic_qos_ok <- request chan $ SimpleMethod $ Basic_qos 0 (fromIntegral prefetchCount) global
  return ()

openConnection :: String -> String -> T.Text -> T.Text -> T.Text -> IO Connection
openConnection host port vhost loginName loginPassword =
  openConnectionOpts
    (defaultConnectionOpts {optServer = (host, port), optNamePass = (loginName, loginPassword)})

openConnectionOpts :: ConnectionOpts -> IO Connection
openConnectionOpts opts = openConnection' opts

closeConnection :: Connection -> IO ()
closeConnection = closeConnection'

openChannel :: Connection -> IO Channel
openChannel = openChannel'

addUnblockedListener :: Connection -> Handler () -> IO ()
addUnblockedListener c action = atomically $ addUnblockedListener' c action

addBlockedListener :: Connection -> Handler T.Text -> IO ()
addBlockedListener c action = atomically $ addBlockedListener' c action
