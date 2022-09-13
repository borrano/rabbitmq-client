{-# OPTIONS -XOverloadedStrings #-}

module Tests.ChannelSpec where

import Channel (Channel (chid), closeChannel)
import Control.Monad
import Data.ByteString.Lazy.Char8 qualified as BL
import Data.IORef
import Lib
import Test.Hspec
import Util

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  describe "openChannel" $ do
    context "with automatically allocated channel id" $ do
      it "opens a new channel with unique id" $ do
        conn <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
        ch1 <- openChannel conn
        ch2 <- openChannel conn
        ch3 <- openChannel conn

        chid ch1 `shouldBe` 1
        chid ch2 `shouldBe` 2
        chid ch3 `shouldBe` 3

        closeConnection conn

  describe "closeChannel" $ do
    context "with an open channel" $ do
      it "closes the channel fix it" $ do
        conn <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
        ch1 <- openChannel conn
        closeChannel ch1
        --
        ch2 <- openChannel conn
        closeChannel ch2
        --
        ch3 <- openChannel conn
        closeChannel ch3
        --
        chid ch1 `shouldBe` 1
        chid ch2 `shouldBe` 1
        chid ch3 `shouldBe` 1
        --
        closeConnection conn

  describe "qos" $ do
    context "with prefetchCount = 5" $ do
      it "sets prefetch count" $ do
        -- we won't demonstrate how basic.qos works in concert
        -- with acks here, it's more of a basic.consume functionality
        -- aspect
        conn <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
        ch <- openChannel conn

        qos ch 3 False
        c <- newIORef 0
        consumeMsgs ch "aa" False (\(_, m, _) -> modifyIORef' c (+ 1))
        forM_ [0 .. 4] $ \i ->
          publishMsg ch "topicExchg" "en.hello" $ newMsg {msgBody = (BL.pack (show i)), msgDeliveryMode = Just NonPersistent}
        threadDelay 3000000
        c' <- readIORef c
        c' `shouldBe` 3

        closeConnection conn
