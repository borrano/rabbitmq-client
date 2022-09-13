{-# LANGUAGE RecordWildCards #-}

module Channel where

import Api
import Control.Exception qualified as E
import Data.ByteString.Lazy qualified as BL
import Data.IORef
import Data.IntSet qualified as S
import Data.Map qualified as M
import Data.Maybe (isJust)
import Data.Sequence qualified as Seq
import Data.Text qualified as T
import Protocol.Types
import Util

type Callback = Handler (Channel, Message, Envelope)

type ConfirmCallback = Handler (Int, Bool, Bool)

type ReturnCallback = Handler (Message, PublishError)

data Confirmation = Confirmation
  { seqNumber :: TVar (Maybe Int), -- if nothing confirm mode is off
    unconfirmedMessages :: TVar (S.IntSet),
    listeners :: TVar (ConfirmCallback)
  }

newConfirmation :: STM Confirmation
newConfirmation = Confirmation <$> newTVar (Nothing) <*> newTVar (S.empty) <*> newTVar (const $ return ())

confirmEnable :: Channel -> STM ()
confirmEnable (Channel {confirmation = Confirmation {..}, ..}) = do
  s <- readTVar seqNumber
  case s of
    Nothing -> writeTVar seqNumber (Just 1)
    _ -> return ()

confirmAdd :: Channel -> STM (Maybe Int)
confirmAdd (Channel {confirmation = Confirmation {..}, ..}) = do
  s <- readTVar seqNumber
  case s of
    Nothing -> return Nothing
    (Just c) -> do
      let nc = c + 1
      writeTVar seqNumber (Just nc) >> modifyTVar unconfirmedMessages (S.insert c)
      return $ Just c

confirmWaitAll :: Channel -> STM ()
confirmWaitAll c@(Channel {..}) = do
  s <- readTVar (unconfirmedMessages confirmation)
  checkOpen c $ check (S.null s)

confirmHandle (Confirmation {..}) tag multiple acked = do
  callback <- readTVar listeners
  when acked $ do
    set <- readTVar (unconfirmedMessages)
    let (l, r) = S.partition (\n -> n <= (fromIntegral $ tag)) set
    writeTVar (unconfirmedMessages) r
  return $ (callback (fromIntegral tag, multiple, acked))

confirmAddListener (Confirmation {..}) cb = addListener listeners cb

data Channel = Channel
  { chid :: ChannelID,
    enabled :: TVar Bool, -- used for flow control
    inChan :: TChan FramePayload,
    outChan :: TChan (ChannelID, Assembly, Maybe (TMVar ())),
    responses :: TVar (Seq.Seq (TMVar Assembly)),
    messageListeners :: TVar (M.Map ConsumerTag (Callback, IO ())),
    consumerTagCounter :: TVar Int,
    confirmation :: Confirmation,
    returnListeners :: TVar (ReturnCallback),
    closed :: TVar (Maybe RabbitMQException),
    cancelAction :: IORef (RabbitMQException -> IO ())
  }

addListener listeners cb = modifyTVar listeners (\f -> (\x -> f x >> (cb x `E.catch` (\(e :: E.SomeException) -> print "error in handler"))))

newChannel chid ioref =
  Channel chid <$> newTVar True <*> newTChan <*> newTChan
    <*> newTVar (Seq.empty)
    <*> newTVar (M.empty)
    <*> newTVar 0
    <*> newConfirmation
    <*> newTVar (const $ return ())
    <*> newTVar Nothing
    <*> pure ioref

addMessageListener (Channel {..}) cb = do
  c <- increment consumerTagCounter
  let tag = ConsumerTag $ T.pack $ show c
  modifyTVar messageListeners (M.insert tag cb)
  return tag

checkOpen :: Channel -> STM b -> STM b
checkOpen ch f = do
  c <- readTVar (closed ch)
  case c of
    (Just x) -> throwSTM x
    Nothing -> f

checkChan ch f = do
  checkOpen ch $ do
    readTVar (enabled ch) >>= check
    f

requestAsyncWait :: Channel -> Assembly -> IO ()
requestAsyncWait c@(Channel {..}) m = do
  tmvar <- atomically $
    checkChan c $ do
      tmvar <- newEmptyTMVar
      writeTChan outChan (chid, m, Just tmvar)
      return tmvar
  atomically $ readTMVar tmvar

requestAsync :: Channel -> Assembly -> STM ()
requestAsync c@(Channel {..}) m = checkChan c $ writeTChan outChan (chid, m, Nothing)

request :: Channel -> Assembly -> IO Assembly
request c@(Channel {..}) m = do
  res <- atomically $ do
    checkChan c $ do
      res <- newEmptyTMVar
      modifyTVar (responses) $ \val -> val Seq.|> res
      writeTChan outChan (chid, m, Nothing)
      return res
  atomically $ checkOpen c $ takeTMVar res

readAssembly :: TChan FramePayload -> STM Assembly
readAssembly chan = do
  m <- readTChan chan
  case m of
    MethodPayload p | hasContent p -> do
      header <- readTChan chan
      case header of
        (ContentHeaderPayload _ _ bodySize props) -> do
          content <- collect $ fromIntegral bodySize
          return $ ContentMethod p props (BL.concat content)
        _ -> error "x"
    MethodPayload p -> return $ SimpleMethod p
    x -> error $ "didn't expect frame: " ++ show x
  where
    hasContent (Basic_get_ok {}) = True
    hasContent (Basic_deliver {}) = True
    hasContent (Basic_return {}) = True
    hasContent _ = False
    collect x | x <= 0 = return []
    collect x = do
      payload <- readTChan chan
      case payload of
        (ContentBodyPayload payload') -> do
          r <- collect (x - BL.length payload')
          return $ payload' : r
        _ -> undefined

channelReceiver :: Channel -> IO b
channelReceiver channel = forever $ join $ atomically $ checkOpen channel $ (readAssembly $ inChan channel) >>= handleAsync channel

handleAsync :: Channel -> Assembly -> STM (IO ())
handleAsync channel (ContentMethod (Basic_deliver (consumerTag) deliveryTag redelivered (exchange) (ShortString routingKey)) properties body) = do
  s <- readTVar (messageListeners channel)
  case M.lookup consumerTag s of
    Just (subscriber, _) -> do
      let msg = createMessage properties body
          env = Envelope deliveryTag redelivered exchange routingKey
      return $ subscriber (channel, msg, env)
    Nothing -> return (return ()) -- got a message, but have no registered subscriber; so drop it
handleAsync channel (SimpleMethod (Basic_ack deliveryTag multiple)) = do
  confirmHandle (confirmation channel) deliveryTag multiple True
handleAsync channel (SimpleMethod (Basic_nack deliveryTag multiple _)) = confirmHandle (confirmation channel) deliveryTag multiple False
handleAsync channel (ContentMethod r@Basic_return {} props body) = do
  let msg = createMessage props body
      pubError = createPublishError r
  callback <- readTVar (returnListeners channel)
  return $ callback (msg, pubError)
handleAsync channel (SimpleMethod (Channel_flow active)) = writeTVar (enabled channel) active >> return (return ())
handleAsync channel (SimpleMethod (Basic_cancel consumerTag _)) = do
  s <- readTVar (messageListeners channel)
  case M.lookup consumerTag (s) of
    Just (_, cb) -> return $ cb
    Nothing -> return (return ()) -- got a message, but have no registered subscriber; so drop it
handleAsync channel (SimpleMethod (Channel_close _ (ShortString errorMsg) _ _)) = return $ do
  requestAsyncWait channel $ SimpleMethod Channel_close_ok
  E.throwIO $ ChannelClosedByServer errorMsg
handleAsync channel assembly = do
  xs <- readTVar (responses channel)
  case Seq.viewl xs of
    x Seq.:< rest -> do
      writeTVar (responses channel) rest
      putTMVar x (assembly)
      return (return ())
    _ -> return (return ())

cancelChannel ch e = readIORef (cancelAction ch) >>= ($ e)

closeChannel :: Channel -> IO ()
closeChannel c = do
  (void $ request c $ SimpleMethod $ Channel_close 0 (ShortString "") 0 0) `E.catch` (cancelChannel c)
  cancelChannel c ChannelClosedByUser
  atomically $ readTVar (closed c) >>= check . isJust -- wait for it to be closed
