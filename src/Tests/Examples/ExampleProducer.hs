module Tests.Examples.ExampleProducer where

import Api
import Channel
import Connection
import Data.ByteString.Lazy.Char8 qualified as BL
import Lib
import Protocol.Types
import Util

main = do
  conn <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
  chan <- openChannel conn

  -- declare queues, exchanges and bindings
  declareQueue chan newQueue {queueName = "myQueueDE"}
  declareQueue chan newQueue {queueName = "myQueueEN"}

  declareExchange chan newExchange {exchangeName = "topicExchg", exchangeType = Topic}
  bindQueue chan "myQueueDE" "topicExchg" "de.*" emptyArgs
  bindQueue chan "myQueueEN" "topicExchg" "en.*" emptyArgs

  -- publish messages
  publishMsg
    chan
    "topicExchg"
    "de.hello"
    ( newMsg
        { msgBody = (BL.pack "hallo welt"),
          msgDeliveryMode = Just NonPersistent
        }
    )
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
