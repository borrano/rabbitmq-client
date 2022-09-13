module Tests.Examples.ExampleConsumer where

import Api
import Channel
import Connection
import Data.ByteString.Lazy.Char8 qualified as BL
import Lib
import Protocol.Types
import Util

x = do
  conn <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
  chan <- openChannel conn

  -- declare queues, exchanges and bindings
  declareQueue chan newQueue {queueName = "myQueueDE"}
  declareQueue chan newQueue {queueName = "myQueueEN"}

  declareExchange chan newExchange {exchangeName = "topicExchg", exchangeType = Topic}
  bindQueue chan "myQueueDE" "topicExchg" "de.*" emptyArgs
  bindQueue chan "myQueueEN" "topicExchg" "en.*" emptyArgs

  -- subscribe to the queues
  consumeMsgs chan "myQueueDE" False myCallbackDE
  consumeMsgs chan "myQueueEN" False myCallbackEN
  publishMsg
    chan
    "topicExchg"
    "en.hello"
    ( newMsg
        { msgBody = (BL.pack "hello world"),
          msgDeliveryMode = Just NonPersistent
        }
    )

  closeConnection conn
  putStrLn "connection closed"
  threadDelay 1000000

myCallbackDE :: (Channel, Message, Envelope) -> IO ()
myCallbackDE (ch, msg, env) = do
  putStrLn $ "received from DE: " ++ (BL.unpack $ msgBody msg)
  ack ch env

myCallbackEN :: (Channel, Message, Envelope) -> IO ()
myCallbackEN (ch, msg, env) = do
  putStrLn $ "received from EN: " ++ (BL.unpack $ msgBody msg)
  ack ch env
