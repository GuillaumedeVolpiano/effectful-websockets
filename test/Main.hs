-- | The client against a websockets server on the loopback interface.
module Main (main) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (withAsync)
import Control.Concurrent.STM (atomically, newTQueueIO, readTQueue, writeTQueue)
import Control.Exception (bracket)
import Control.Monad (forM_, forever, replicateM)
import Data.ByteString qualified as ByteString
import Data.List (sort)
import Data.Text (Text)
import Data.Text qualified as Text
import Effectful
import Effectful.Concurrent (Concurrent, runConcurrent)
import Effectful.Concurrent.Async (forConcurrently_)
import Effectful.Error.Static (Error, runErrorNoCallStack)
import Effectful.State.Static.Local qualified as Local
import Effectful.WebSocket
import Effectful.WebSocket.Client
import Network.Socket qualified as Socket
import Network.WebSockets qualified as WS
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream qualified as Stream
import Test.Tasty (TestTree, defaultMain, localOption, mkTimeout, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests =
  localOption (mkTimeout 20_000_000) $
    testGroup
      "effectful-websockets"
      [ testCase "echoes text and binary messages" $ do
          outcome <- withServer echo $ \config -> client config $ do
            _ <- sendText "héllo"
            _ <- sendBinary (ByteString.pack [0, 1, 255])
            replicateM 2 receiveMessage
          outcome @?= Right [Just (TextMessage "héllo"), Just (BinaryMessage (ByteString.pack [0, 1, 255]))]
      , testCase "carries a message larger than one packet" $ do
          let large = ByteString.replicate 300_000 42
          outcome <- withServer echo $ \config -> client config (sendBinary large >> receiveMessage)
          outcome @?= Right (Just (BinaryMessage large))
      , testCase "streams messages until the server closes" $ do
          let server connection = forM_ ["a", "b", "c"] (WS.sendTextData connection . Text.pack) >> WS.sendClose connection ("" :: Text) >> drain connection
          outcome <- withServer server $ \config -> client config (Stream.fold Fold.toList messages)
          outcome @?= Right [TextMessage "a", TextMessage "b", TextMessage "c"]
      , testCase "sending after the server closed fails" $ do
          let server connection = WS.sendClose connection ("" :: Text) >> drain connection
          outcome <- withServer server $ \config -> client config $ do
            _ <- receiveMessage
            liftIO (threadDelay 100_000)
            sendText "late"
          outcome @?= Right False
      , testCase "reports a refused connection" $ do
          port <- freePort
          outcome <- client (clientConfig "127.0.0.1" port "/") (pure ())
          case outcome of
            Left (ConnectFailed _) -> pure ()
            other -> fail ("unexpected outcome: " <> show other)
      , testCase "reports a server that stops answering pings" $ do
          -- This server never reads, so it never answers a ping.
          let server _ = forever (threadDelay 1_000_000)
          outcome <- withServer server $ \config ->
            client config {pingPong = Just PingPong {interval = 1, timeout = 2}} receiveMessage
          outcome @?= Left NoPong
      , testCase "keeps the local state of the action" $ do
          outcome <- withServer echo $ \config ->
            client config . Local.execState (0 :: Int) $ do
              Local.modify @Int (+ 1)
              _ <- sendText "x" >> receiveMessage
              Local.modify @Int (+ 1)
          outcome @?= Right 2
      , testCase "sends from several threads without mixing messages" $ do
          let payloads = [ByteString.replicate 50_000 (fromIntegral n) | n <- [1 .. 20 :: Int]]
          received <- newTQueueIO
          let server connection = forever (WS.receiveData connection >>= atomically . writeTQueue received)
          outcome <- withServer server $ \config ->
            client config (forConcurrently_ payloads sendBinary)
          outcome @?= Right ()
          messagesReceived <- replicateM 20 (atomically (readTQueue received))
          sort messagesReceived @?= sort payloads
      ]

-- | Echo every message.
echo :: WS.Connection -> IO ()
echo connection = forever (WS.receiveDataMessage connection >>= WS.sendDataMessage connection)

-- | Read until the client closes.
drain :: WS.Connection -> IO ()
drain connection = forever (WS.receiveDataMessage connection)

client
  :: ClientConfig
  -> Eff '[WebSocket, Error WebSocketError, Concurrent, IOE] a
  -> IO (Either WebSocketError a)
client config = runEff . runConcurrent . runErrorNoCallStack . runWebSocketClient config

-- | Serve each connection with a handler, on a free loopback port, while
-- the client runs. Retries the client while the server is not listening.
withServer
  :: (WS.Connection -> IO ())
  -> (ClientConfig -> IO (Either WebSocketError a))
  -> IO (Either WebSocketError a)
withServer handler run = do
  port <- freePort
  let app pending = WS.acceptRequest pending >>= handler
  withAsync (WS.runServer "127.0.0.1" port app) $ \_ -> attempt (50 :: Int) (clientConfig "127.0.0.1" port "/")
  where
    attempt tries config =
      run config >>= \case
        Left (ConnectFailed _) | tries > 0 -> threadDelay 20_000 >> attempt (tries - 1) config
        outcome -> pure outcome

-- | A port that was free a moment ago.
freePort :: IO Int
freePort =
  bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \socket -> do
    Socket.bind socket (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
    fromIntegral <$> Socket.socketPort socket
