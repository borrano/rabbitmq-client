module Tests.CustomTest where

import Data.ByteString.Lazy.Char8 qualified as BL
import Lib
import Protocol.Types
import Util

test1 = tt2

exName1 = "ex1"

qName1 = "q3"

tt2 = do
  c <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
  chan <- openChannel c
  declareExchange chan newExchange {exchangeName = exName1, exchangeType = Topic}
  declareQueue chan newQueue {queueName = qName1}

  bindQueue chan qName1 exName1 "en.*" emptyArgs
  publishMsg chan exName1 "en.hello" $ newMsg {msgBody = "a", msgDeliveryMode = Just Persistent}
  --_ <- consumeMsgs chan qName1 False ((\(a, m, e) -> print e >> ack a e))
  threadDelay 1000000
  closeConnection c

tt = do
  c <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
  chan <- openChannel c
  declareExchange chan newExchange {exchangeName = "topicExchg", exchangeType = Topic}
  x <- declareQueue chan newQueue {queueName = "aa"}
  bindQueue chan "aa" "topicExchg" "en.*" emptyArgs

  _ <- consumeMsgs chan "aa" False ((\(a, m, e) -> print e))
  publishMsg chan "topicExchg" "en.hello" $ newMsg {msgBody = "a", msgDeliveryMode = Just NonPersistent}
  x <- declareQueue chan newQueue {queueName = "aa"}

  print x
  threadDelay 1000000
  return ()

test1' :: IO ()
test1' = do
  c <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
  chan <- openChannel c
  _ <- declareQueue chan newQueue {queueName = "aa"}
  declareExchange chan newExchange {exchangeName = "topicExchg", exchangeType = Topic}
  bindQueue chan "aa" "topicExchg" "en.*" emptyArgs
  addConfirmationListener chan confirmCb
  -- addReturnListener chan retCb
  _ <- consumeMsgs chan "aa" False consumer
  confirmSelect chan True
  forM_ [0 .. 10] $ \i ->
    publishMsg chan "topicExchg" "en.hello" $ newMsg {msgBody = (BL.pack (show i)), msgDeliveryMode = Just NonPersistent}
  _ <- publishMsg' chan "1topicExchg" "x" True $ newMsg {msgBody = (BL.pack (show 111)), msgDeliveryMode = Just NonPersistent}
  waitForConfirms chan
  threadDelay 2000000
  where
    consumer (c, m, e) = print (msgBody m) >> ack c e
    confirmCb x = do
      (print $ "xxxxxxxxxxxxxxxconfirmed" ++ show x)
    retCb x = print x

test2 = do
  c <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
  chan <- openChannel c
  _ <- declareQueue chan newQueue {queueName = "aa"}
  declareExchange chan newExchange {exchangeName = "topicExchg", exchangeType = Topic}
  bindQueue chan "aa" "topicExchg" "en.*" emptyArgs
  _ <- consumeMsgs' chan "aa" False cb (print "cancelled") emptyArgs
  _ <- forkIO $ deleteQueue chan "aa" >>= print
  threadDelay 2000000
  where
    cb (c, m, e) = print (msgBody m) >> ack c e
