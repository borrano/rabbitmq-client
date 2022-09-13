module Tests.BasicRejectSpec where

import Api
import Channel
import Connection
import Control.Concurrent (threadDelay)
import Control.Exception (bracket)
import Data.ByteString.Lazy.Char8 as L8
import Lib
import Test.Hspec

main :: IO ()
main = hspec spec

withTestConnection :: (Channel -> IO c) -> IO c
withTestConnection job = do
  bracket (openConnection "127.0.0.1" "5672" "/" "guest" "guest") closeConnection $ \conn -> do
    ch <- openChannel conn
    job ch

spec :: Spec
spec = do
  describe "rejectMsg" $ do
    context "requeue = True" $ do
      it "requeues a message" $
        withTestConnection $ \ch -> do
          let q = "haskell-amqp.basic.reject.with-requeue-true"

          (_, n1, _) <- declareQueue ch $ newQueue {queueName = q, queueDurable = False}
          n1 `shouldBe` 0

          -- publishes using default exchange
          publishMsg ch "" q $ newMsg {msgBody = (L8.pack "hello")}
          threadDelay (1000 * 100)

          (_, n2, _) <- declareQueue ch (newQueue {queueName = q, queueDurable = False})
          n2 `shouldBe` 1

          Just (_msg, env) <- getMsg ch False q
          rejectMsg (ch) (envDeliveryTag env) True
          threadDelay (1000 * 100)

          n3 <- deleteQueue ch q
          n3 `shouldBe` 1

    context "requeue = False" $ do
      it "rejects a message" $
        withTestConnection $ \ch -> do
          let q = "haskell-amqp.basic.reject.with-requeue-false"

          (_, n1, _) <- declareQueue ch $ newQueue {queueName = q, queueDurable = False}
          n1 `shouldBe` 0

          -- publishes using default exchange
          publishMsg ch "" q $ newMsg {msgBody = (L8.pack "hello")}
          threadDelay (1000 * 100)

          (_, n2, _) <- declareQueue ch (newQueue {queueName = q, queueDurable = False})
          n2 `shouldBe` 1

          Just (_msg, env) <- getMsg ch False q
          rejectMsg (ch) (envDeliveryTag env) False
          threadDelay (1000 * 100)

          n3 <- deleteQueue ch q
          n3 `shouldBe` 0

  describe "rejectEnv" $ do
    context "requeue = True" $ do
      it "requeues a message" $
        withTestConnection $ \ch -> do
          let q = "haskell-amqp.basic.reject.with-requeue-true"

          (_, n1, _) <- declareQueue ch $ newQueue {queueName = q, queueDurable = False}
          n1 `shouldBe` 0

          -- publishes using default exchange
          publishMsg ch "" q $ newMsg {msgBody = (L8.pack "hello")}
          threadDelay (1000 * 100)

          (_, n2, _) <- declareQueue ch (newQueue {queueName = q, queueDurable = False})
          n2 `shouldBe` 1

          Just (_msg, env) <- getMsg ch False q
          reject ch env True
          threadDelay (1000 * 100)

          n3 <- deleteQueue ch q
          n3 `shouldBe` 1

    context "requeue = False" $ do
      it "rejects a message" $
        withTestConnection $ \ch -> do
          let q = "haskell-amqp.basic.reject.with-requeue-false"

          (_, n1, _) <- declareQueue ch $ newQueue {queueName = q, queueDurable = False}
          n1 `shouldBe` 0

          -- publishes using default exchange
          publishMsg ch "" q $ newMsg {msgBody = (L8.pack "hello")}
          threadDelay (1000 * 100)

          (_, n2, _) <- declareQueue ch (newQueue {queueName = q, queueDurable = False})
          n2 `shouldBe` 1

          Just (_msg, env) <- getMsg ch False q
          reject ch env False
          threadDelay (1000 * 100)

          n3 <- deleteQueue ch q
          n3 `shouldBe` 0
