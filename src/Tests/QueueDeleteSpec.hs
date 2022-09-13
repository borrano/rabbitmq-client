{-# OPTIONS -XOverloadedStrings #-}

module Tests.QueueDeleteSpec where

import Control.Concurrent (threadDelay)
import Lib
import Test.Hspec

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  describe "deleteQueue" $ do
    context "when queue exists" $ do
      it "deletes the queue" $ do
        conn <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
        ch <- openChannel conn
        -- silence error messages
        -- addChannelExceptionHandler ch $ return . const ()
        (q, _, _) <- declareQueue ch $ newQueue {queueName = "haskell-amqp.queues.to-be-deleted", queueExclusive = True}
        n <- deleteQueue ch q
        n `shouldBe` 0
        let ex = (ChannelClosedByServer "NOT_FOUND - no queue 'haskell-amqp.queues.to-be-deleted' in vhost '/'")
        (declareQueue ch $ newQueue {queueName = q, queuePassive = True}) `shouldThrow` (== ex)
        closeConnection conn

      context "when queue DOES NOT exist" $ do
        it "throws an exception" $ do
          conn <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
          ch <- openChannel conn
          -- silence error messages
          -- addChannelExceptionHandler ch $ return . const ()

          let q = "haskell-amqp.queues.GmN8rozyXiz2mQYcFrQg"
              ex = ChannelClosedByServer "NOT_FOUND - no queue 'haskell-amqp.queues.GmN8rozyXiz2mQYcFrQg' in vhost '/'"
          (declareQueue ch $ newQueue {queueName = q, queuePassive = True}) `shouldThrow` (== ex)

          closeConnection conn
