{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RecordWildCards #-}

module Connection (Connection, openConnection', closeConnection', openChannel', addUnblockedListener', addBlockedListener') where

import Api
import Channel hiding (checkOpen)
import Control.Exception qualified as E
import Data.Bit.ThreadSafe qualified as B
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BC
import Data.ByteString.Lazy qualified as BL
import Data.Foldable (traverse_)
import Data.IORef
import Data.IntMap qualified as IM
import Data.Map qualified as M
import Data.Maybe
import Data.Text qualified as T
import Data.Text.Encoding qualified as T
import Data.Vector.Generic qualified as V
import Data.Vector.Unboxed.Mutable qualified as MV
import Network.Socket
import Network.Socket.ByteString (sendAll)
import Protocol.Serialization
import Protocol.Types
import Util

data Connection = Connection
  { maxFrameSize :: Int,
    handle :: Socket,
    chanAllocator :: ChanAllocator,
    output :: TChan (ChannelID, Assembly, Maybe (TMVar ())),
    channels :: TVar (IM.IntMap Channel),
    blockedListeners :: TVar (Handler T.Text),
    unblockedListeners :: TVar (Handler ()),
    lastSent :: IORef (Integer),
    lastReceived :: IORef (Integer),
    connClosed :: TVar (Maybe RabbitMQException),
    connCancel :: IORef (RabbitMQException -> IO ())
  }

checkOpen conn f = do
  c <- readTVar (connClosed conn)
  case c of
    (Just x) -> throwSTM x
    Nothing -> f

---- allocator
data ChanAllocator = ChanAllocator
  { bitvec :: MVar (MV.IOVector B.Bit)
  }

newAllocator :: Int -> IO ChanAllocator
newAllocator size = ChanAllocator <$> (MV.replicate size 0 >>= newMVar)

allocate :: ChanAllocator -> IO (Maybe Int)
allocate (ChanAllocator bitvec) = withMVar bitvec $ \v -> do
  v' <- V.basicUnsafeFreeze v
  case B.nthBitIndex 0 2 v' of
    Nothing -> return Nothing
    (Just i) -> MV.modify v (const 1) i >> return (Just i)

free :: ChanAllocator -> Int -> IO ()
free (ChanAllocator bitvec) i = withMVar bitvec (\v -> MV.modify v (const 0) i)

--- heartbeats

updateLastSent conn = do
  a <- getTimeMicro
  atomicModifyIORef (lastSent conn) (\_ -> (a, ()))

updateLastReceived conn = do
  a <- getTimeMicro
  atomicModifyIORef (lastReceived conn) (\_ -> (a, ()))

sendRequest conn x =
  atomically $
    writeTChan (output conn) (0, x, Nothing)

watchHeartbeats :: Connection -> Int -> IO ()
watchHeartbeats conn timeout =
  forever $ do
    threadDelay 100000
    go (lastSent conn) sendTimeout $ sendRequest conn Heartbeat
    go (lastReceived conn) receiveTimeout $ E.throwIO ConnectionMissingHeartbeat
  where
    rate = timeout * 1000 * 1000 -- timeout / 4 in µs
    receiveTimeout = fromIntegral rate * 2 -- 2*timeout in µs
    sendTimeout = fromIntegral rate `div` 2 -- timeout/2 in µs
    go ref time action = do
      currentTime <- getTimeMicro
      saved <- atomicModifyIORef (ref) (\a -> (a, a))
      when (currentTime >= saved + time) $ action

networkHandler :: E.IOException -> IO a
networkHandler e = E.throwIO NetworkError

readFrame :: Socket -> IO Frame
readFrame handle = do
  x <- (BL.fromStrict) <$> (recvExact handle 7) `E.catch` networkHandler
  let len = fromIntegral $ peekFrameSize x
  y <- (BL.fromStrict) <$> (recvExact handle (len + 1)) `E.catch` networkHandler
  case decodeOrFail (x <> y) of
    (Right (_, _, a)) -> return a
    x -> E.throwIO ProtocolError

writeFrame :: Socket -> Frame -> IO ()
writeFrame handle f = (sendAll handle . BS.toStrict . runPut $ (put f)) `E.catch` networkHandler

forwarder :: Connection -> IO b
forwarder conn@(Connection {..}) = forever $ do
  Frame chanID payload <- readFrame (handle)
  updateLastReceived conn
  (join . atomically) $ forwardToChannel chanID payload
  where
    forwardToChannel 0 (MethodPayload (MethodConnection (Connection_close_ok))) = return $ E.throwIO ConnectionClosedByUser
    forwardToChannel 0 (MethodPayload (MethodConnection (Connection_close _ (ShortString errorMsg) _ _))) = return $ E.throwIO ConnectionClosedByServer
    forwardToChannel 0 (MethodPayload (MethodConnection (Connection_blocked (ShortString reason)))) = do
      callback <- readTVar (blockedListeners)
      return $ callback reason
    forwardToChannel 0 (MethodPayload (MethodConnection Connection_unblocked)) = do
      callback <- readTVar (unblockedListeners)
      return $ callback ()
    forwardToChannel 0 HeartbeatPayload = return (return ())
    forwardToChannel 0 x = return $ print x
    forwardToChannel chanID payload = do
      channels' <- readTVar channels
      let channel = channels' IM.! (fromIntegral chanID)
      writeTChan (inChan channel) payload
      return (return ())

sender :: Connection -> IO b
sender conn@(Connection {..}) = forever $ do
  (chanId, asm, tmvar) <- atomically $ do
    chans <- fmap (fmap outChan . IM.elems) $ readTVar channels
    foldl orElse (readTChan output) (readTChan <$> chans)
  case tmvar of
    Nothing -> return ()
    (Just tmvar') -> atomically $ putTMVar tmvar' ()
  updateLastSent conn
  let frames = Frame chanId <$> (assemblyToFrames maxFrameSize asm)
  mapM_ (writeFrame handle) frames

handshake h (ConnectionOpts {..}) = do
  sendAll h $ (BC.pack "AMQP") <> (BS.pack [1, 1, 0, 9])
  Frame 0 (MethodPayload (MethodConnection (Connection_start _ _ serverProps (LongString serverMechanisms) _))) <- readFrame h
  writeFrame h (initFrame optNamePass)
  (Frame 0 (MethodPayload (MethodConnection Connection_tune {..}))) <- readFrame h
  let channel_max' = channel_max -- tune parameters here -- add it later
      frame_max' = frame_max
      heartbeat_interval' = heartbeat_interval
  writeFrame h $ makeFrame (Connection_tune_ok channel_max' frame_max' heartbeat_interval')
  writeFrame h $ makeFrame $ Connection_open (ShortString $ "/") (ShortString $ T.pack "") True
  (Frame 0 (MethodPayload (MethodConnection (Connection_open_ok _)))) <- readFrame h
  return (channel_max', frame_max', heartbeat_interval')
  where
    initFrame (name, pass) =
      makeFrame $
        Connection_start_ok
          clientProperties
          (ShortString $ "PLAIN")
          (LongString $ T.encodeUtf8 $ (T.cons '\0' name) <> (T.cons '\0' pass))
          (ShortString "en_US")
      where
        clientProperties = FieldTable $ M.fromList $ [("platform", FVString "Haskell"), ("capabilities", FVFieldTable clientCapabilities)]
        clientCapabilities = FieldTable $ M.fromList [("consumer_cancel_notify", FVBool True), ("connection.blocked", FVBool True)]
    makeFrame = Frame 0 . MethodPayload . MethodConnection

openConnection' :: ConnectionOpts -> IO Connection
openConnection' opts@(ConnectionOpts {optServer = (host, port), ..}) = do
  s <- createSocket (host) (port)
  (channel_max, frame_max, heartbeat_interval) <- handshake s opts
  allocator <- newAllocator (fromIntegral $ channel_max)
  time <- getTimeMicro
  connection <-
    Connection (fromIntegral frame_max) s allocator <$> newTChanIO
      <*> (newTVarIO IM.empty)
      <*> newTVarIO (const $ return ())
      <*> newTVarIO (const $ return ())
      <*> newIORef time
      <*> newIORef time
      <*> newTVarIO Nothing
      <*> newIORef (\_ -> return ())
  return connection
  tid <- forkFinally ((sender connection) `race` (forwarder connection) `race` (watchHeartbeats connection (fromIntegral heartbeat_interval))) (closeHandler connection)
  writeIORef (connCancel connection) (E.throwTo tid)
  return connection
  where
    closeHandler conn (Left e) = case E.fromException e of
      Just (e' :: RabbitMQException) -> do
        channels <- atomically $ do
          modifyTVar (connClosed conn) (maybe (Just e') Just)
          readTVar $ channels conn
        traverse_ (\c -> cancelChannel c e') channels
      _ -> E.throwIO e

closeConnection' :: Connection -> IO ()
closeConnection' conn = do
  (sendRequest conn $ SimpleMethod $ MethodConnection $ Connection_close 0 (ShortString "") 0 0) `E.catch` (cancel')
  print "x"
  atomically $ readTVar (connClosed conn) >>= check . isJust -- wait for it to be closed
  where
    cancel' e = readIORef (connCancel conn) >>= ($ e)

openChannel' :: Connection -> IO Channel
openChannel' conn@(Connection {..}) = do
  chid <- allocate (chanAllocator)
  case chid of
    Nothing -> E.throwIO MaxChannelReached
    (Just chid) -> do
      c <- newIORef (\_ -> return ())
      channel <- atomically $
        checkOpen conn $ do
          -- checkOpen here if an exception occurs before  adding to map no way to not to leak channel threads
          channel <- newChannel (fromIntegral chid) c
          modifyTVar (channels) (IM.insert (fromIntegral chid) channel)
          return channel
      tid <- forkFinally (channelReceiver channel) (closeHandler channel)
      writeIORef c (E.throwTo tid)
      SimpleMethod (Channel_open_ok _) <- request channel $ SimpleMethod $ Channel_open (ShortString "")
      return channel
  where
    closeHandler channel (Left e) = do
      case E.fromException e of
        Just (e' :: RabbitMQException) -> do
          atomically $ do
            modifyTVar (closed channel) (maybe (Just e') Just)
            modifyTVar (channels) (IM.delete (fromIntegral $ chid channel))
          free chanAllocator (fromIntegral $ chid channel)
        _ -> E.throwIO e

addUnblockedListener' :: Connection -> (Handler ()) -> STM ()
addUnblockedListener' c action = addListener (unblockedListeners c) (action)

addBlockedListener' :: Connection -> (Handler T.Text) -> STM ()
addBlockedListener' c action = addListener (blockedListeners c) (action)