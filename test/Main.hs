-- | The client against a server of streamly-websockets on the loopback
-- interface.
module Main (main) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (withAsync)
import Control.Concurrent.STM (atomically, newTQueueIO, readTQueue, writeTQueue)
import Control.Exception (bracket)
import Control.Monad (forM_, forever, replicateM)
import Data.ByteString qualified as ByteString
import Data.IORef (atomicModifyIORef', modifyIORef', newIORef, readIORef)
import Data.List (sort)
import Effectful
import Effectful.Concurrent (Concurrent, runConcurrent)
import Effectful.Concurrent.Async (forConcurrently_)
import Effectful.Error.Static (Error, runErrorNoCallStack)
import Effectful.State.Static.Local qualified as Local
import Effectful.WebSocket
import Effectful.WebSocket.Client
import Network.Socket qualified as Socket
import Streamly.Data.Fold qualified as Fold
import Streamly.Data.Stream qualified as Stream
import Streamly.External.Network.WebSocket.Server qualified as Server
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
          let server connection = forM_ ["a", "b", "c"] (Server.send connection . Server.TextMessage) >> Server.close connection >> drain connection
          outcome <- withServer server $ \config -> client config (Stream.fold Fold.toList messages)
          outcome @?= Right [TextMessage "a", TextMessage "b", TextMessage "c"]
      , testCase "sending after the server closed fails" $ do
          let server connection = Server.close connection >> drain connection
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
      , testCase "traced reports the traffic of an in-memory connection" $ do
          (log', inbox) <- (,) <$> newIORef [] <*> newIORef [Just (TextMessage "one"), Nothing]
          let receive = liftIO (atomicModifyIORef' inbox (\case (x : rest) -> (rest, x); [] -> ([], Nothing)))
              observe traffic = liftIO (modifyIORef' log' (traffic :))
          received <- runEff . runWebSocketWith (\_ -> pure True) receive . traced observe $
            sendText "hello" >> (,) <$> receiveMessage <*> receiveMessage
          received @?= (Just (TextMessage "one"), Nothing)
          reverse <$> readIORef log' >>= (@?= [Sent (TextMessage "hello") True, Received (Just (TextMessage "one")), Received Nothing])
      , testCase "traced reports the traffic of a real connection" $ do
          log' <- newIORef []
          outcome <- withServer echo $ \config ->
            client config . traced (\traffic -> liftIO (modifyIORef' log' (traffic :))) $
              sendBinary (ByteString.pack [1, 2]) >> receiveMessage
          outcome @?= Right (Just (BinaryMessage (ByteString.pack [1, 2])))
          reverse <$> readIORef log' >>= (@?= [Sent (BinaryMessage (ByteString.pack [1, 2])) True, Received (Just (BinaryMessage (ByteString.pack [1, 2])))])
      , testCase "sends from several threads without mixing messages" $ do
          let payloads = [ByteString.replicate 50_000 (fromIntegral n) | n <- [1 .. 20 :: Int]]
          received <- newTQueueIO
          let keep = \case
                Server.BinaryMessage bytes -> atomically (writeTQueue received bytes)
                Server.TextMessage _ -> pure ()
              server connection = Stream.fold (Fold.drainMapM keep) (Server.messages connection)
          outcome <- withServer server $ \config ->
            client config (forConcurrently_ payloads sendBinary)
          outcome @?= Right ()
          messagesReceived <- replicateM 20 (atomically (readTQueue received))
          sort messagesReceived @?= sort payloads
      ]

-- | Echo every message.
echo :: Server.Connection -> IO ()
echo connection = Stream.fold (Fold.drainMapM (Server.send connection)) (Server.messages connection)

-- | Read until the client closes.
drain :: Server.Connection -> IO ()
drain connection = Stream.fold Fold.drain (Server.messages connection)

client
  :: ClientConfig
  -> Eff '[WebSocket, Error WebSocketError, Concurrent, IOE] a
  -> IO (Either WebSocketError a)
client config = runEff . runConcurrent . runErrorNoCallStack . runWebSocketClient config

-- | Serve each connection with a handler, on a free loopback port, while
-- the client runs.
withServer
  :: (Server.Connection -> IO ())
  -> (ClientConfig -> IO (Either WebSocketError a))
  -> IO (Either WebSocketError a)
withServer handler run =
  bracket (Server.listen "127.0.0.1" 0) Socket.close $ \listener -> do
    port <- Socket.socketPort listener
    withAsync (Server.serveSocket listener Server.defaultServerOptions (const handler)) $ \_ ->
      run (clientConfig "127.0.0.1" (fromIntegral port) "/")

-- | A port that was free a moment ago.
freePort :: IO Int
freePort =
  bracket (Socket.socket Socket.AF_INET Socket.Stream Socket.defaultProtocol) Socket.close $ \socket -> do
    Socket.bind socket (Socket.SockAddrInet 0 (Socket.tupleToHostAddress (127, 0, 0, 1)))
    fromIntegral <$> Socket.socketPort socket
