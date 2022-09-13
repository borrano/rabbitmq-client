{-# OPTIONS -XOverloadedStrings #-}

module Tests.ExchangeDeleteSpec where

import Lib
import Test.Hspec
import Protocol.Types

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  describe "deleteExchange" $ do
    context "when exchange exists" $ do
      it "deletes the exchange" $ do
        let eName = "haskell-amqp.exchanges.to-be-deleted"

        conn <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
        ch <- openChannel conn
        _ <- declareExchange ch (newExchange {exchangeName = eName, exchangeType = Topic, exchangeDurable = False})

        _ <- deleteExchange ch eName
        let ex = ChannelClosedByServer "NOT_FOUND - no exchange 'haskell-amqp.exchanges.to-be-deleted' in vhost '/'"
        (declareExchange ch $ newExchange {exchangeName = eName, exchangePassive = True}) `shouldThrow` (== ex)
        closeConnection conn

    context "when exchange DOES NOT exist" $ do
      it "throws an exception" $ do
        conn <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
        ch <- openChannel conn

        let q = "haskell-amqp.exchanges.GmN8rozyXiz2mQYcFrQg"
            ex = ChannelClosedByServer "NOT_FOUND - no exchange 'haskell-amqp.exchanges.GmN8rozyXiz2mQYcFrQg' in vhost '/'"
        (declareExchange ch $ newExchange {exchangeName = q, exchangePassive = True}) `shouldThrow` (== ex)

        closeConnection conn