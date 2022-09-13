module Util
  ( module Data.Binary,
    module Data.Binary.Put,
    module Control.Concurrent.STM,
    module Control.Concurrent,
    module Control.Concurrent.Async,
    module Control.Monad,
    module GHC.Generics,
    module Util,
  )
where

import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Exception qualified as E
import Control.Monad
import Data.Binary
import Data.Binary.Put
import Data.ByteString qualified as BS
import Data.Time.Clock.System
import GHC.Generics
import Network.Socket
import Network.Socket.ByteString (recv)

type Handler a = a -> IO ()

recvExact :: Socket -> Int -> IO BS.ByteString
recvExact conn x = loop BS.empty 0
  where
    loop bs y
      | y == x = return bs
      | otherwise = do
          next <- recv conn (x - y)
          loop (BS.append bs next) (y + (BS.length next))

createSocket :: HostName -> ServiceName -> IO Socket
createSocket host port = withSocketsDo $ do
  let hints = defaultHints {addrSocketType = Stream}
  addr <- head <$> getAddrInfo (Just hints) (Just host) (Just port)
  open addr
  where
    open addr = E.bracketOnError (openSocket addr) close $ \sock -> do
      connect sock $ addrAddress addr
      return sock

increment :: Num b => TVar b -> STM b
increment ref = do
  a <- readTVar ref
  writeTVar ref (a + 1)
  return (a + 1)

getTimeMicro :: IO Integer
getTimeMicro = do
  a1 <- getSystemTime
  let (sec :: Integer) = fromIntegral $ systemSeconds a1
      (nsec :: Integer) = fromIntegral $ systemNanoseconds a1
  return $ (sec * (10 ^ 6)) + (nsec `div` 1000)
