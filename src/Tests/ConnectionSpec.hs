{-# OPTIONS -XOverloadedStrings #-}

module Tests.ConnectionSpec where

import Lib
import Test.Hspec

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  describe "openConnection" $ do
    context "with default vhost and default admin credentials" $ do
      it "connects successfully" $ do
        conn <- openConnection "127.0.0.1" "5672" "/" "guest" "guest"
        closeConnection conn
