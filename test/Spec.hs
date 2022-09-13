import Test.Hspec
import Tests.BasicPublishSpec qualified
import Tests.BasicRejectSpec qualified
import Tests.ChannelSpec qualified
import Tests.ConnectionSpec qualified
import Tests.ExchangeDeclareSpec qualified
import Tests.ExchangeDeleteSpec qualified
import Tests.QueueDeclareSpec qualified
import Tests.QueueDeleteSpec qualified
import Tests.QueuePurgeSpec qualified

main :: IO ()
main =
  hspec $ do
    describe "ConnectionSpec" Tests.ConnectionSpec.spec
    describe "BasicPublishSpec" Tests.BasicPublishSpec.spec
    describe "BasicRejectSpec" Tests.BasicRejectSpec.spec
    describe "ChannelSpec" Tests.ChannelSpec.spec
    describe "ExchangeDeclareSpec" Tests.ExchangeDeclareSpec.spec
    describe "ExchangeDeleteSpec" Tests.ExchangeDeleteSpec.spec
    describe "QueueDeclareSpec" Tests.QueueDeclareSpec.spec
    describe "QueueDeleteSpec" Tests.QueueDeleteSpec.spec
    describe "QueuePurgeSpec" Tests.QueuePurgeSpec.spec