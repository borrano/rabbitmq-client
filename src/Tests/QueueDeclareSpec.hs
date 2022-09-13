{-# OPTIONS -XOverloadedStrings #-}

module Tests.QueueDeclareSpec where

import Control.Concurrent (threadDelay)
import Data.Text (isPrefixOf)
import Lib
import Protocol.Types
import Test.Hspec

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  describe "declareQueue" $ do
    context "client named, durable, non-autodelete, non-exclusive" $ do
      it "declares the queue" $ do
        let qName = "haskell-amqp.client-named.d.na.ne"
        conn <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
        ch <- openChannel conn
        (_, _, _) <- declareQueue ch (newQueue {queueName = qName, queueDurable = True, queueExclusive = False, queueAutoDelete = False})
        -- ensure the queue was declared
        (_, _, _) <- declareQueue ch (newQueue {queueName = qName, queuePassive = True})
        closeConnection conn

    context "server- named, non-durable, non-autodelete, exclusive" $ do
      it "declares the queue, providing access to the server-generated name" $ do
        let qName = ""
        conn <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
        ch <- openChannel conn
        (q@(QueueName qn), _, _) <- declareQueue ch (newQueue {queueName = qName, queueDurable = False, queueExclusive = True, queueAutoDelete = False})
        (_, cn, mn) <- declareQueue ch (newQueue {queueName = q, queuePassive = True})
        (isPrefixOf "amq.gen" qn) `shouldBe` True
        -- consumer count, undelivered message count
        cn `shouldBe` 0
        mn `shouldBe` 0
        closeConnection conn

    context "passive declaration when the queue DOES NOT exist" $ do
      it "throws an exception" $ do
        conn <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
        ch <- openChannel conn
        -- silence error messages
        -- addChannelExceptionHandler ch $ return . const ()
        let x = "haskell-amqp.queues.mQozyYcFrQgGmN8Xiz2r"
            ex = ChannelClosedByServer "NOT_FOUND - no queue 'haskell-amqp.queues.mQozyYcFrQgGmN8Xiz2r' in vhost '/'"
        (declareQueue ch $ newQueue {queueName = x, queuePassive = True}) `shouldThrow` (== ex)
        closeConnection conn

--