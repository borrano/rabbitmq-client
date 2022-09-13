{-# LANGUAGE DerivingVia #-}

module Protocol.Serialization where

import Control.Monad
import Data.Binary
import Data.Binary.Get
import Data.Binary.IEEE754
import Data.Binary.Put
import Data.ByteString qualified as BS
import Data.ByteString.Lazy.Char8 qualified as BL
import Data.Char
import Data.Int
import Data.Map qualified as M
import Data.Maybe (isJust)
import Data.Text.Encoding qualified as T
import GHC.Bits
import Protocol.Types

instance Binary ExhangeType where
  get = do
    ((ShortString s)) <- get
    case s of
      "fanout" -> return Fanout
      "direct" -> return Direct
      "topic" -> return Topic
      "headers" -> return Headers
  put (Fanout) = put (ShortString "fanout")
  put (Direct) = put (ShortString "direct")
  put (Topic) = put (ShortString "topic")
  put (Headers) = put (ShortString "headers")

-- | (must be set); the type of the exchange (\"fanout\", \"direct\", \"topic\", \"headers\")
assemblyToFrames :: Integral p => p -> Assembly -> [FramePayload]
assemblyToFrames frameSize (ContentMethod m properties msg) =
  [MethodPayload m, ContentHeaderPayload (getClassIDOf properties) 0 (fromIntegral $ BL.length msg) properties] ++ rest
  where
    rest = if BL.length msg > 0 then do map ContentBodyPayload (splitLen msg $ fromIntegral (frameSize) - 8) else []
    splitLen str len | BL.length str > len = BL.take len str : splitLen (BL.drop len str) len
    splitLen str _ = [str]
assemblyToFrames frameSize (SimpleMethod m) = [MethodPayload m]
assemblyToFrames frameSize (Heartbeat) = [HeartbeatPayload]

deriving via (ShortString) instance (Binary) QueueName

deriving via (ShortString) instance (Binary) ConsumerTag

deriving via (ShortString) instance (Binary) ExchangeName

instance Binary Frame where
  get = do
    fType <- getWord8
    channel <- get
    payloadSize <- get
    payload <- getPayload fType payloadSize
    0xCE <- getWord8 -- frame end
    return $ Frame channel payload

  put (Frame chan payload) = do
    putWord8 $ frameType payload
    put chan
    let buf = runPut $ putPayload payload
    put ((fromIntegral $ BL.length buf) :: PayloadSize)
    putLazyByteString buf
    putWord8 0xCE

frameType :: FramePayload -> Word8
frameType MethodPayload {} = 1
frameType ContentHeaderPayload {} = 2
frameType ContentBodyPayload {} = 3
frameType HeartbeatPayload = 8

-- gets the size of the frame
-- the bytestring should be at least 7 bytes long, otherwise this method will fail
peekFrameSize = runGet $ do
  void getWord8 -- 1 byte
  void (get :: Get ChannelID) -- 2 bytes
  get :: Get PayloadSize -- 4 bytes

--
getPayload :: Word8 -> PayloadSize -> Get FramePayload
getPayload 1 _ = do
  -- METHOD FRAME
  payLoad <- get :: Get MethodPayload
  return (MethodPayload payLoad)
getPayload 2 _ = do
  -- content header frame
  classID <- get :: Get ShortInt
  weight <- get :: Get ShortInt
  bodySize <- get :: Get LongLongInt
  props <- getContentHeaderProperties classID
  return (ContentHeaderPayload classID weight bodySize props)
getPayload 3 payloadSize = do
  -- content body frame
  payload <- getLazyByteString $ fromIntegral payloadSize
  return (ContentBodyPayload payload)
getPayload 8 payloadSize = do
  -- ignoring the actual payload, but still need to read the bytes from the network buffer
  _ <- getLazyByteString $ fromIntegral payloadSize
  return HeartbeatPayload
getPayload n _ = error ("Unknown frame payload: " ++ show n)

--
putPayload :: FramePayload -> Put
putPayload (MethodPayload payload) = put payload
putPayload (ContentHeaderPayload classID weight bodySize p) = do
  put classID
  put weight
  put bodySize
  putContentHeaderProperties p
putPayload (ContentBodyPayload payload) = putLazyByteString payload
putPayload HeartbeatPayload = putLazyByteString BL.empty

--

getContentHeaderProperties :: ShortInt -> Get ContentHeaderProperties
getContentHeaderProperties 10 = getPropBits 0 >>= \[] -> return CHConnection
getContentHeaderProperties 20 = getPropBits 0 >>= \[] -> return CHChannel
getContentHeaderProperties 40 = getPropBits 0 >>= \[] -> return CHExchange
getContentHeaderProperties 50 = getPropBits 0 >>= \[] -> return CHQueue
getContentHeaderProperties 60 = getPropBits 14 >>= \[a, b, c, d, e, f, g, h, i, j, k, l, m, n] -> condGet a >>= \a' -> condGet b >>= \b' -> condGet c >>= \c' -> condGet d >>= \d' -> condGet e >>= \e' -> condGet f >>= \f' -> condGet g >>= \g' -> condGet h >>= \h' -> condGet i >>= \i' -> condGet j >>= \j' -> condGet k >>= \k' -> condGet l >>= \l' -> condGet m >>= \m' -> condGet n >>= \n' -> return (CHBasic a' b' c' d' e' f' g' h' i' j' k' l' m' n')
getContentHeaderProperties 90 = getPropBits 0 >>= \[] -> return CHTx
getContentHeaderProperties 85 = getPropBits 0 >>= \[] -> return CHConfirm
getContentHeaderProperties n = error ("Unexpected content header properties: " ++ show n)

putContentHeaderProperties :: ContentHeaderProperties -> Put
putContentHeaderProperties CHConnection = putPropBits []
putContentHeaderProperties CHChannel = putPropBits []
putContentHeaderProperties CHExchange = putPropBits []
putContentHeaderProperties CHQueue = putPropBits []
putContentHeaderProperties (CHBasic a b c d e f g h i j k l m n) = do
  putPropBits [isJust a, isJust b, isJust c, isJust d, isJust e, isJust f, isJust g, isJust h, isJust i, isJust j, isJust k, isJust l, isJust m, isJust n]
  condPut a >> condPut b >> condPut c >> condPut d >> condPut e >> condPut f >> condPut g >> condPut h >> condPut i >> condPut j >> condPut k >> condPut l >> condPut m >> condPut n
putContentHeaderProperties CHTx = putPropBits []
putContentHeaderProperties CHConfirm = putPropBits []

putBits :: [Bit] -> Put
putBits = putWord8 . putBits' 0

putBits' :: Int -> [Bit] -> Word8
putBits' _ [] = 0
putBits' offset (x : xs) = (shiftL (toInt x) offset) .|. (putBits' (offset + 1) xs)
  where
    toInt True = 1
    toInt False = 0

getBits :: Int -> Get [Bit]
getBits num = getWord8 >>= return . getBits' num 0

getBits' :: Int -> Int -> Word8 -> [Bit]
getBits' 0 _ _ = []
getBits' num offset x = ((x .&. (2 ^ offset)) /= 0) : (getBits' (num - 1) (offset + 1) x)

-- | Packs up to 15 Bits into a Word16 (=Property Flags)
putPropBits :: [Bit] -> Put
putPropBits = putWord16be . putPropBits' 0
  where
    putPropBits' :: Int -> [Bit] -> Word16
    putPropBits' _ [] = 0
    putPropBits' offset (x : xs) = (shiftL (toInt x) (15 - offset)) .|. (putPropBits' (offset + 1) xs)
      where
        toInt True = 1
        toInt False = 0

getPropBits :: Int -> Get [Bit]
getPropBits num = getWord16be >>= return . getPropBits' num 0
  where
    getPropBits' :: Int -> Int -> Word16 -> [Bit]
    getPropBits' 0 _ _ = []
    getPropBits' num offset x = ((x .&. (2 ^ (15 - offset))) /= 0) : (getPropBits' (num - 1) (offset + 1) x)

condGet :: Binary a => Bool -> Get (Maybe a)
condGet False = return Nothing
condGet True = get >>= return . Just

condPut :: Binary a => Maybe a -> Put
condPut = maybe (return ()) put

readMany :: (Show a, Binary a) => BL.ByteString -> [a]
readMany = runGet (readMany' [] 0)

readMany' :: (Show a, Binary a) => [a] -> Int -> Get [a]
readMany' _ 1000 = error "readMany overflow"
readMany' acc overflow = do
  x <- get
  emp <- isEmpty
  if not emp
    then readMany' (x : acc) (overflow + 1)
    else return (x : acc)

putMany :: Binary a => [a] -> PutM ()
putMany = mapM_ put

instance Binary ShortString where
  get = do
    len <- getWord8
    dat <- getByteString (fromIntegral len)
    return $ ShortString $ T.decodeUtf8 dat

  put (ShortString x) = do
    let s = T.encodeUtf8 x
    if BS.length s > 255
      then error "cannot encode ShortString with length > 255"
      else do
        putWord8 $ fromIntegral (BS.length s)
        putByteString s

instance Binary FieldTable where
  get = do
    len <- get :: Get LongInt -- length of fieldValuePairs in bytes
    if len > 0
      then do
        fvp <- getLazyByteString (fromIntegral len)
        let !fields = readMany fvp
        return $ FieldTable $ M.fromList $ map (\(ShortString a, b) -> (a, b)) fields
      else return $ FieldTable M.empty

  put (FieldTable fvp) = do
    let bytes = runPut (putMany $ map (\(a, b) -> (ShortString a, b)) $ M.toList fvp) :: BL.ByteString
    put ((fromIntegral $ BL.length bytes) :: LongInt)
    putLazyByteString bytes

instance Binary FieldValue where
  get = do
    fieldType <- getWord8
    case chr $ fromIntegral fieldType of
      't' -> FVBool <$> get
      'b' -> FVInt8 <$> get
      's' -> FVInt16 <$> get
      'I' -> FVInt32 <$> get
      'l' -> FVInt64 <$> get
      'f' -> FVFloat <$> getFloat32be
      'd' -> FVDouble <$> getFloat64be
      'D' -> FVDecimal <$> get
      'S' -> do
        LongString x <- get :: Get LongString
        return $ FVString x
      'A' -> do
        len <- get :: Get Int32
        if len > 0
          then do
            fvp <- getLazyByteString (fromIntegral len)
            let !fields = readMany fvp
            return $ FVFieldArray fields
          else return $ FVFieldArray []
      'T' -> FVTimestamp <$> get
      'F' -> FVFieldTable <$> get
      'V' -> return FVVoid
      'x' -> do
        len <- get :: Get Word32
        FVByteArray <$> getByteString (fromIntegral len)
      -- this should never happen:
      c -> error ("Unknown field type: " ++ show c)

  put (FVBool x) = put 't' >> put x
  put (FVInt8 x) = put 'b' >> put x
  put (FVInt16 x) = put 's' >> put x
  put (FVInt32 x) = put 'I' >> put x
  put (FVInt64 x) = put 'l' >> put x
  put (FVFloat x) = put 'f' >> putFloat32be x
  put (FVDouble x) = put 'd' >> putFloat64be x
  put (FVDecimal x) = put 'D' >> put x
  put (FVString x) = put 'S' >> put (LongString x)
  put (FVFieldArray x) = do
    put 'A'
    if null x
      then put (0 :: Int32)
      else do
        let bytes = runPut (putMany x) :: BL.ByteString
        put ((fromIntegral $ BL.length bytes) :: Int32)
        putLazyByteString bytes
  put (FVTimestamp s) = put 'T' >> put s
  put (FVFieldTable s) = put 'F' >> put s
  put FVVoid = put 'V'
  put (FVByteArray x) = do
    put 'x'
    let len = fromIntegral (BS.length x) :: Word32
    put len
    putByteString x

instance Binary DecimalValue where
  get = do
    a <- getWord8
    b <- get :: Get LongInt
    return $ DecimalValue a b

  put (DecimalValue a b) = put a >> put b

instance Binary LongString where
  get = do
    len <- getWord32be
    dat <- getByteString (fromIntegral len)
    return $ LongString dat

  put (LongString x) = do
    putWord32be $ fromIntegral (BS.length x)
    putByteString x

instance Binary MethodConnection where
  put (Connection_start a b c d e) = putWord16be 10 >> put a >> put b >> put c >> put d >> put e
  put (Connection_start_ok a b c d) = putWord16be 11 >> put a >> put b >> put c >> put d
  put (Connection_secure a) = putWord16be 20 >> put a
  put (Connection_secure_ok a) = putWord16be 21 >> put a
  put (Connection_tune a b c) = putWord16be 30 >> put a >> put b >> put c
  put (Connection_tune_ok a b c) = putWord16be 31 >> put a >> put b >> put c
  put (Connection_open a b c) = putWord16be 40 >> put a >> put b >> put c
  put (Connection_open_ok a) = putWord16be 41 >> put a
  put (Connection_close a b c d) = putWord16be 50 >> put a >> put b >> put c >> put d
  put Connection_close_ok = putWord16be 51
  put (Connection_blocked a) = putWord16be 60 >> put a
  put Connection_unblocked = putWord16be 61
  get = do
    methodID <- getWord16be
    case methodID of
      10 -> Connection_start <$> get <*> get <*> get <*> get <*> get
      11 -> Connection_start_ok <$> get <*> get <*> get <*> get
      20 -> get >>= \a -> return (Connection_secure a)
      21 -> get >>= \a -> return (Connection_secure_ok a)
      30 -> get >>= \a -> get >>= \b -> get >>= \c -> return (Connection_tune a b c)
      31 -> get >>= \a -> get >>= \b -> get >>= \c -> return (Connection_tune_ok a b c)
      40 -> get >>= \a -> get >>= \b -> get >>= \c -> return (Connection_open a b c)
      41 -> get >>= \a -> return (Connection_open_ok a)
      50 -> Connection_close <$> get <*> get <*> get <*> get
      51 -> return Connection_close_ok
      60 -> Connection_blocked <$> get
      61 -> return Connection_unblocked

instance Binary MethodPayload where
  put (MethodConnection c) = putWord16be 10 >> put c
  put (Channel_open a) = putWord16be 20 >> putWord16be 10 >> put a
  put (Channel_open_ok a) = putWord16be 20 >> putWord16be 11 >> put a
  put (Channel_flow a) = putWord16be 20 >> putWord16be 20 >> put a
  put (Channel_flow_ok a) = putWord16be 20 >> putWord16be 21 >> put a
  put (Channel_close a b c d) = putWord16be 20 >> putWord16be 40 >> put a >> put b >> put c >> put d
  put Channel_close_ok = putWord16be 20 >> putWord16be 41
  put (Exchange_declare a b c d e f g h i) = putWord16be 40 >> putWord16be 10 >> put a >> put b >> put c >> putBits [d, e, f, g, h] >> put i
  put Exchange_declare_ok = putWord16be 40 >> putWord16be 11
  put (Exchange_delete a b c d) = putWord16be 40 >> putWord16be 20 >> put a >> put b >> putBits [c, d]
  put Exchange_delete_ok = putWord16be 40 >> putWord16be 21
  put (Exchange_bind a b c d e f) = putWord16be 40 >> putWord16be 30 >> put a >> put b >> put c >> put d >> put e >> put f
  put Exchange_bind_ok = putWord16be 40 >> putWord16be 31
  put (Exchange_unbind a b c d e f) = putWord16be 40 >> putWord16be 40 >> put a >> put b >> put c >> put d >> put e >> put f
  put Exchange_unbind_ok = putWord16be 40 >> putWord16be 51
  put (Queue_declare a b c d e f g h) = putWord16be 50 >> putWord16be 10 >> put a >> put b >> putBits [c, d, e, f, g] >> put h
  put (Queue_declare_ok a b c) = putWord16be 50 >> putWord16be 11 >> put a >> put b >> put c
  put (Queue_bind a b c d e f) = putWord16be 50 >> putWord16be 20 >> put a >> put b >> put c >> put d >> put e >> put f
  put Queue_bind_ok = putWord16be 50 >> putWord16be 21
  put (Queue_unbind a b c d e) = putWord16be 50 >> putWord16be 50 >> put a >> put b >> put c >> put d >> put e
  put Queue_unbind_ok = putWord16be 50 >> putWord16be 51
  put (Queue_purge a b c) = putWord16be 50 >> putWord16be 30 >> put a >> put b >> put c
  put (Queue_purge_ok a) = putWord16be 50 >> putWord16be 31 >> put a
  put (Queue_delete a b c d e) = putWord16be 50 >> putWord16be 40 >> put a >> put b >> putBits [c, d, e]
  put (Queue_delete_ok a) = putWord16be 50 >> putWord16be 41 >> put a
  put (Basic_qos a b c) = putWord16be 60 >> putWord16be 10 >> put a >> put b >> put c
  put Basic_qos_ok = putWord16be 60 >> putWord16be 11
  put (Basic_consume a b c d e f g h) = putWord16be 60 >> putWord16be 20 >> put a >> put b >> put c >> putBits [d, e, f, g] >> put h
  put (Basic_consume_ok a) = putWord16be 60 >> putWord16be 21 >> put a
  put (Basic_cancel a b) = putWord16be 60 >> putWord16be 30 >> put a >> put b
  put (Basic_cancel_ok a) = putWord16be 60 >> putWord16be 31 >> put a
  put (Basic_publish a b c d e) = putWord16be 60 >> putWord16be 40 >> put a >> put b >> put c >> putBits [d, e]
  put (Basic_return a b c d) = putWord16be 60 >> putWord16be 50 >> put a >> put b >> put c >> put d
  put (Basic_deliver a b c d e) = putWord16be 60 >> putWord16be 60 >> put a >> put b >> put c >> put d >> put e
  put (Basic_get a b c) = putWord16be 60 >> putWord16be 70 >> put a >> put b >> put c
  put (Basic_get_ok a b c d e) = putWord16be 60 >> putWord16be 71 >> put a >> put b >> put c >> put d >> put e
  put (Basic_get_empty a) = putWord16be 60 >> putWord16be 72 >> put a
  put (Basic_ack a b) = putWord16be 60 >> putWord16be 80 >> put a >> put b
  put (Basic_reject a b) = putWord16be 60 >> putWord16be 90 >> put a >> put b
  put (Basic_recover_async a) = putWord16be 60 >> putWord16be 100 >> put a
  put (Basic_recover a) = putWord16be 60 >> putWord16be 110 >> put a
  put Basic_recover_ok = putWord16be 60 >> putWord16be 111
  put (Basic_nack a b c) = putWord16be 60 >> putWord16be 120 >> put a >> putBits [b, c]
  put Tx_select = putWord16be 90 >> putWord16be 10
  put Tx_select_ok = putWord16be 90 >> putWord16be 11
  put Tx_commit = putWord16be 90 >> putWord16be 20
  put Tx_commit_ok = putWord16be 90 >> putWord16be 21
  put Tx_rollback = putWord16be 90 >> putWord16be 30
  put Tx_rollback_ok = putWord16be 90 >> putWord16be 31
  put (Confirm_select a) = putWord16be 85 >> putWord16be 10 >> put a
  put Confirm_select_ok = putWord16be 85 >> putWord16be 11
  get = do
    classID <- getWord16be
    if classID == 10
      then MethodConnection <$> get
      else do
        methodID <- getWord16be
        case (classID, methodID) of
          (20, 10) -> get >>= \a -> return (Channel_open a)
          (20, 11) -> get >>= \a -> return (Channel_open_ok a)
          (20, 20) -> get >>= \a -> return (Channel_flow a)
          (20, 21) -> get >>= \a -> return (Channel_flow_ok a)
          (20, 40) -> get >>= \a -> get >>= \b -> get >>= \c -> get >>= \d -> return (Channel_close a b c d)
          (20, 41) -> return Channel_close_ok
          (40, 10) -> get >>= \a -> get >>= \b -> get >>= \c -> getBits 5 >>= \[d, e, f, g, h] -> get >>= \i -> return (Exchange_declare a b c d e f g h i)
          (40, 11) -> return Exchange_declare_ok
          (40, 20) -> get >>= \a -> get >>= \b -> getBits 2 >>= \[c, d] -> return (Exchange_delete a b c d)
          (40, 21) -> return Exchange_delete_ok
          (40, 30) -> get >>= \a -> get >>= \b -> get >>= \c -> get >>= \d -> get >>= \e -> get >>= \f -> return (Exchange_bind a b c d e f)
          (40, 31) -> return Exchange_bind_ok
          (40, 40) -> get >>= \a -> get >>= \b -> get >>= \c -> get >>= \d -> get >>= \e -> get >>= \f -> return (Exchange_unbind a b c d e f)
          (40, 51) -> return Exchange_unbind_ok
          (50, 10) -> get >>= \a -> get >>= \b -> getBits 5 >>= \[c, d, e, f, g] -> get >>= \h -> return (Queue_declare a b c d e f g h)
          (50, 11) -> get >>= \a -> get >>= \b -> get >>= \c -> return (Queue_declare_ok a b c)
          (50, 20) -> get >>= \a -> get >>= \b -> get >>= \c -> get >>= \d -> get >>= \e -> get >>= \f -> return (Queue_bind a b c d e f)
          (50, 21) -> return Queue_bind_ok
          (50, 50) -> get >>= \a -> get >>= \b -> get >>= \c -> get >>= \d -> get >>= \e -> return (Queue_unbind a b c d e)
          (50, 51) -> return Queue_unbind_ok
          (50, 30) -> get >>= \a -> get >>= \b -> get >>= \c -> return (Queue_purge a b c)
          (50, 31) -> get >>= \a -> return (Queue_purge_ok a)
          (50, 40) -> get >>= \a -> get >>= \b -> getBits 3 >>= \[c, d, e] -> return (Queue_delete a b c d e)
          (50, 41) -> get >>= \a -> return (Queue_delete_ok a)
          (60, 10) -> get >>= \a -> get >>= \b -> get >>= \c -> return (Basic_qos a b c)
          (60, 11) -> return Basic_qos_ok
          (60, 20) -> get >>= \a -> get >>= \b -> get >>= \c -> getBits 4 >>= \[d, e, f, g] -> get >>= \h -> return (Basic_consume a b c d e f g h)
          (60, 21) -> get >>= \a -> return (Basic_consume_ok a)
          (60, 30) -> get >>= \a -> get >>= \b -> return (Basic_cancel a b)
          (60, 31) -> get >>= \a -> return (Basic_cancel_ok a)
          (60, 40) -> get >>= \a -> get >>= \b -> get >>= \c -> getBits 2 >>= \[d, e] -> return (Basic_publish a b c d e)
          (60, 50) -> get >>= \a -> get >>= \b -> get >>= \c -> get >>= \d -> return (Basic_return a b c d)
          (60, 60) -> get >>= \a -> get >>= \b -> get >>= \c -> get >>= \d -> get >>= \e -> return (Basic_deliver a b c d e)
          (60, 70) -> get >>= \a -> get >>= \b -> get >>= \c -> return (Basic_get a b c)
          (60, 71) -> get >>= \a -> get >>= \b -> get >>= \c -> get >>= \d -> get >>= \e -> return (Basic_get_ok a b c d e)
          (60, 72) -> get >>= \a -> return (Basic_get_empty a)
          (60, 80) -> get >>= \a -> get >>= \b -> return (Basic_ack a b)
          (60, 90) -> get >>= \a -> get >>= \b -> return (Basic_reject a b)
          (60, 100) -> get >>= \a -> return (Basic_recover_async a)
          (60, 110) -> get >>= \a -> return (Basic_recover a)
          (60, 111) -> return Basic_recover_ok
          (60, 120) -> get >>= \a -> getBits 2 >>= \[b, c] -> return (Basic_nack a b c)
          (90, 10) -> return Tx_select
          (90, 11) -> return Tx_select_ok
          (90, 20) -> return Tx_commit
          (90, 21) -> return Tx_commit_ok
          (90, 30) -> return Tx_rollback
          (90, 31) -> return Tx_rollback_ok
          (85, 10) -> get >>= \a -> return (Confirm_select a)
          (85, 11) -> return Confirm_select_ok
          x -> error ("Unexpected classID and methodID: " ++ show x)

getClassIDOf :: ContentHeaderProperties -> ShortInt
getClassIDOf (CHConnection) = 10
getClassIDOf (CHChannel) = 20
getClassIDOf (CHExchange) = 40
getClassIDOf (CHQueue) = 50
getClassIDOf (CHBasic {}) = 60
getClassIDOf (CHTx) = 90
getClassIDOf (CHConfirm) = 85