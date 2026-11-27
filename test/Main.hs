-- | The client against a server of streamly-websockets on the loopback
-- interface.
module Main (main) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (withAsync)
import Control.Concurrent.STM (atomically, newTQueueIO, readTQueue, writeTQueue)
import Control.Exception (bracket)
import Control.Monad (forM_, forever, replicateM)
import Data.ByteString (pack)
import Data.ByteString qualified as BS (replicate)
import Data.IORef (atomicModifyIORef', modifyIORef', newIORef, readIORef)
import Data.List (sort)
import Effectful (Eff, IOE, MonadIO (liftIO), runEff)
import Effectful.Concurrent (Concurrent, runConcurrent)
import Effectful.Concurrent.Async (forConcurrently_)
import Effectful.Error.Static (Error, runErrorNoCallStack)
import Effectful.State.Static.Local (execState, modify)
import Effectful.WebSocket (Message (BinaryMessage, TextMessage),
                            Traffic (Received, Sent), WebSocket, messages,
                            receiveMessage, runWebSocketWith, sendBinary,
                            sendText, traced)
import Effectful.WebSocket.Client (ClientConfig (pingPong),
                                   PingPong (PingPong, interval, timeout),
                                   WebSocketError (..), clientConfig,
                                   runWebSocketClient)
import Network.Socket (Family (AF_INET), SockAddr (SockAddrInet),
                       SocketType (Stream), bind, defaultProtocol, socketPort,
                       tupleToHostAddress)
import Network.Socket qualified as N (close, socket)
import Streamly.Data.Fold (drainMapM, toList)
import Streamly.Data.Fold qualified as F (drain)
import Streamly.Data.Stream (fold)
import Streamly.External.Network.WebSocket.Server (Connection,
                                                   defaultServerOptions, listen,
                                                   send, serveSocket)
import Streamly.External.Network.WebSocket.Server qualified as WSS (Message (BinaryMessage, TextMessage),
                                                                    close,
                                                                    messages)
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
            _ <- sendBinary (pack [0, 1, 255])
            replicateM 2 receiveMessage
          outcome @?= Right [Just (TextMessage "héllo"), Just (BinaryMessage (pack [0, 1, 255]))]
      , testCase "carries a message larger than one packet" $ do
          let large = BS.replicate 300_000 42
          outcome <- withServer echo $ \config -> client config (sendBinary large >> receiveMessage)
          outcome @?= Right (Just (BinaryMessage large))
      , testCase "streams messages until the server closes" $ do
          let server connection = forM_ ["a", "b", "c"] (send connection . WSS.TextMessage) >> WSS.close connection >> drain connection
          outcome <- withServer server $ \config -> client config (fold toList messages)
          outcome @?= Right [TextMessage "a", TextMessage "b", TextMessage "c"]
      , testCase "sending after the server closed fails" $ do
          let server connection = WSS.close connection >> drain connection
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
            client config . execState (0 :: Int) $ do
              modify @Int (+ 1)
              _ <- sendText "x" >> receiveMessage
              modify @Int (+ 1)
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
              sendBinary (pack [1, 2]) >> receiveMessage
          outcome @?= Right (Just (BinaryMessage (pack [1, 2])))
          reverse <$> readIORef log' >>= (@?= [Sent (BinaryMessage (pack [1, 2])) True, Received (Just (BinaryMessage (pack [1, 2])))])
      , testCase "sends from several threads without mixing messages" $ do
          let payloads = [BS.replicate 50_000 (fromIntegral n) | n <- [1 .. 20 :: Int]]
          received <- newTQueueIO
          let keep = \case
                WSS.BinaryMessage bytes -> atomically (writeTQueue received bytes)
                WSS.TextMessage _ -> pure ()
              server connection = fold (drainMapM keep) (WSS.messages connection)
          outcome <- withServer server $ \config ->
            client config (forConcurrently_ payloads sendBinary)
          outcome @?= Right ()
          messagesReceived <- replicateM 20 (atomically (readTQueue received))
          sort messagesReceived @?= sort payloads
      ]

-- | Echo every message.
echo :: Connection -> IO ()
echo connection = fold (drainMapM (send connection)) (WSS.messages connection)

-- | Read until the client closes.
drain :: Connection -> IO ()
drain connection = fold F.drain (WSS.messages connection)

client
  :: ClientConfig
  -> Eff '[WebSocket, Error WebSocketError, Concurrent, IOE] a
  -> IO (Either WebSocketError a)
client config = runEff . runConcurrent . runErrorNoCallStack . runWebSocketClient config

-- | Serve each connection with a handler, on a free loopback port, while
-- the client runs.
withServer
  :: (Connection -> IO ())
  -> (ClientConfig -> IO (Either WebSocketError a))
  -> IO (Either WebSocketError a)
withServer handler run =
  bracket (listen "127.0.0.1" 0) N.close $ \listener -> do
    port <- socketPort listener
    withAsync (serveSocket listener defaultServerOptions (const handler)) $ \_ ->
      run (clientConfig "127.0.0.1" (fromIntegral port) "/")

-- | A port that was free a moment ago.
freePort :: IO Int
freePort =
  bracket (N.socket AF_INET Stream defaultProtocol) N.close $ \socket -> do
    bind socket (SockAddrInet 0 (tupleToHostAddress (127, 0, 0, 1)))
    fromIntegral <$> socketPort socket
