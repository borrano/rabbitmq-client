{-# OPTIONS -XOverloadedStrings #-}

module Tests.BasicPublishSpec where

import Api
import Data.ByteString.Lazy.Char8 as BL
import Data.Map qualified as Map
import Lib
import Protocol.Types
import Test.Hspec
import Util

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  describe "publishMsg" $ do
    context "with a routing key" $ do
      it "publishes a message" $ do
        let q = "haskell-amqp.queues.publish-over-default-exchange1"
        conn <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
        ch <- openChannel conn
        (_, n1, _) <- declareQueue ch (newQueue {queueName = q, queueDurable = False})
        n1 `shouldBe` 0
        publishMsg ch "" q (newMsg {msgBody = (BL.pack "hello")})
        threadDelay (1000 * 100)
        (_, n2, _) <- declareQueue ch (newQueue {queueName = q, queueDurable = False})
        n2 `shouldBe` 1
        n3 <- deleteQueue ch q
        n3 `shouldBe` 1
        closeConnection conn

    context "with a blank routing key" $ do
      it "publishes a message" $ do
        let q = "haskell-amqp.queues.publish-over-fanout1"
            e = "haskell-amqp.fanout.d.na"
        conn <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
        ch <- openChannel conn
        _ <- declareExchange ch $ newExchange {exchangeName = e, exchangeType = Fanout, exchangeDurable = True}
        (_, _, _) <- declareQueue ch (newQueue {queueName = q, queueDurable = False})
        _ <- purgeQueue ch q
        bindQueue ch q e "" emptyArgs
        publishMsg ch e "" (newMsg {msgBody = (BL.pack "hello")})
        threadDelay (1000 * 100)
        (_, n, _) <- declareQueue ch (newQueue {queueName = q, queueDurable = False})
        n `shouldBe` 1
        _ <- deleteQueue ch q
        closeConnection conn

    context "confirmSelect" $ do
      it "receives a confirmation message" $ do
        let q = "haskell-amqp.queues.publish-over-fanout1"
            e = "haskell-amqp.fanout.d.na"
        conn <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
        ch <- openChannel conn
        confirmSelect ch True
        (confirmMap, counter) <- atomically $ (,) <$> newTVar Map.empty <*> newTVar 0
        addConfirmationListener ch (handleConfirms counter confirmMap)
        _ <-
          declareExchange
            ch
            ( newExchange
                { exchangeName = e,
                  exchangeType = Fanout,
                  exchangeDurable = True
                }
            )
        (_, _, _) <- declareQueue ch (newQueue {queueName = q, queueDurable = False})
        _ <- purgeQueue ch q
        bindQueue ch q e "" emptyArgs
        _ <- forM [1 .. 5] $ \n -> do
          sn' <- publishMsg ch e "" (newMsg {msgBody = (BL.pack "hello")})
          case sn' of
            Just sn -> atomically $ addSequenceNumber confirmMap (fromIntegral sn) n
            Nothing -> return ()

        --
        threadDelay (1000 * 100)
        --
        (_, n, _) <- declareQueue ch (newQueue {queueName = q, queueDurable = False})
        n `shouldBe` 5
        --
        cMap' <- atomically $ readTVar confirmMap
        cMap' `shouldBe` Map.empty
        --
        counter' <- atomically $ readTVar counter
        counter' `shouldBe` 5
        --
        _ <- deleteQueue ch q
        closeConnection conn

addSequenceNumber cMap sn n = modifyTVar' cMap (Map.insert sn n)

removeSequenceNumber cMap sn = modifyTVar' cMap (Map.delete sn)

increaseCounter n = modifyTVar' n (+ 1)

handleConfirms c _ (_, False, False) = atomically $ increaseCounter c
handleConfirms c _ (_, True, False) = atomically $ increaseCounter c
handleConfirms c cMap (n, False, True) = atomically $ removeSequenceNumber cMap n >> increaseCounter c
handleConfirms c cMap (n, True, True) = atomically $ do
  cMap' <- readTVar cMap
  let (lt, eq', _) = Map.splitLookup n cMap'
  case eq' of
    Just _ -> removeSequenceNumber cMap n >> increaseCounter c
    Nothing -> return ()
  _ <- traverse (\i -> removeSequenceNumber cMap i >> increaseCounter c) (Map.keys lt)
  return ()
