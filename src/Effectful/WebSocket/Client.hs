-- |
-- Module      : Effectful.WebSocket.Client
-- Description : Opening a WebSocket to a server
-- License     : BSD-3-Clause
--
-- A client over the @streamly-websockets@ package, on plain TCP (no TLS).
--
-- The action runs in the thread of the caller, so that effects with
-- thread-local state, such as a local @State@, keep the changes it makes.
-- The pings are sent by a watchdog thread.
module Effectful.WebSocket.Client
  ( -- * Configuration
    ClientConfig (..)
  , clientConfig
  , PingPong (..)
  , defaultPingPong

    -- * Errors
  , WebSocketError (..)

    -- * Interpreter
  , runWebSocketClient

    -- * Other ways to open the connection
  , runWebSocketOver
  , connectionConfig
  ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (ExceptionInLinkedThread (..), link, withAsync)
import Control.Concurrent.STM (atomically, check, readTVar, registerDelay)
import Control.Exception (Exception, IOException, SomeException, catch, displayException, fromException, throwIO, try)
import Control.Monad (forever, unless)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as Char8
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Text (Text)
import Data.Text qualified as Text
import Effectful
import Effectful.Error.Static (Error, throwError)
import Effectful.WebSocket (Message (..), WebSocket, runWebSocketWith)
import Streamly.External.Network.HTTP (HttpError)
import Streamly.External.Network.WebSocket qualified as WS

-- | Where to connect, and how.
data ClientConfig = ClientConfig
  { host :: String
  , port :: Int
  , path :: String
  -- ^ Such as @\/api@.
  , headers :: [(ByteString, ByteString)]
  -- ^ Extra headers for the opening handshake: names and values.
  , maxMessageSize :: Int
  -- ^ The largest message accepted, in bytes. A larger one ends the
  -- connection.
  , pingPong :: Maybe PingPong
  -- ^ 'Nothing' sends no ping.
  , closeTimeout :: Int
  -- ^ Microseconds to wait, once the action is done, for the server to
  -- answer the closing of the connection.
  }

-- | A configuration with no extra header, messages of at most 64 MiB,
-- 'defaultPingPong', and one second to close.
clientConfig :: String -> Int -> String -> ClientConfig
clientConfig host port path =
  ClientConfig
    { host
    , port
    , path
    , headers = []
    , maxMessageSize = 64 * 1024 * 1024
    , pingPong = Just defaultPingPong
    , closeTimeout = 1_000_000
    }

-- | Keeping the connection alive, and noticing when the server is gone.
data PingPong = PingPong
  { interval :: Int
  -- ^ Seconds between two pings.
  , timeout :: Int
  -- ^ Seconds without a pong before the connection counts as lost.
  }
  deriving stock (Eq, Show)

-- | A ping every ten seconds, twenty seconds to answer.
defaultPingPong :: PingPong
defaultPingPong = PingPong {interval = 10, timeout = 20}

data WebSocketError
  = -- | The connection could not be opened. Carries the cause.
    ConnectFailed Text
  | -- | The server did not answer the pings in time.
    NoPong
  deriving stock (Eq, Show)

-- | Thrown by the watchdog when a ping gets no pong in time.
data PongTimeout = PongTimeout
  deriving stock (Show)

instance Exception PongTimeout

-- | Open a connection and run an action over it. The connection is closed
-- when the action returns.
--
-- Pongs are only seen while the action receives messages: an action that
-- stops receiving for longer than the timeout loses the connection.
--
-- Throws 'ConnectFailed' if the connection cannot be opened, and 'NoPong'
-- if the server stops answering pings. A connection closed by the server
-- is not an error: receiving returns 'Nothing' and sending 'False'. So
-- does a server that breaks the protocol or sends too large a message.
runWebSocketClient
  :: (IOE :> es, Error WebSocketError :> es)
  => ClientConfig
  -> Eff (WebSocket : es) a
  -> Eff es a
runWebSocketClient config = runWebSocketOver (WS.withConnection (connectionConfig config)) config.pingPong

-- | The configuration of @streamly-websockets@ that a 'ClientConfig'
-- stands for, without its pings.
connectionConfig :: ClientConfig -> WS.Config
connectionConfig config =
  (WS.config config.host config.port (Char8.pack config.path))
    { WS.headers = config.headers
    , WS.maxMessageSize = config.maxMessageSize
    , WS.closeTimeout = config.closeTimeout
    }

-- | Like 'runWebSocketClient', with the way to open the connection left
-- to a function, such as @withConnection@ of @streamly-websockets@ over
-- another transport: @effectful-websockets-tls@ uses it for secure
-- WebSockets. The pings are given apart.
--
-- What the function throws before it has a connection is reported as
-- 'ConnectFailed' when it is an 'IOError', an error of
-- @streamly-websockets@ or one of @streamly-http@; anything else passes.
runWebSocketOver
  :: (IOE :> es, Error WebSocketError :> es)
  => (forall x. (WS.Connection -> IO x) -> IO x)
  -> Maybe PingPong
  -> Eff (WebSocket : es) a
  -> Eff es a
runWebSocketOver open pings action = do
  outcome <- withEffToIO SeqUnlift $ \runInIO -> do
    connected <- newIORef False
    attempt <- try . open $ \connection -> do
      writeIORef connected True
      closed <- newIORef False
      let run = runInIO (runWebSocketWith (liftIO . sendTo closed connection) (liftIO (receiveFrom closed connection)) action)
      case pings of
        Nothing -> run
        Just pingPong -> withAsync (watch pingPong connection) $ \watchdog -> link watchdog >> run
    wasConnected <- readIORef connected
    case attempt of
      Right value -> pure (Right value)
      Left (err :: SomeException)
        | not wasConnected, openingFailure err -> pure (Left (ConnectFailed (Text.pack (displayException err))))
        | Just (ExceptionInLinkedThread _ cause) <- fromException err
        , Just PongTimeout <- fromException cause ->
            pure (Left NoPong)
        | otherwise -> throwIO err
  either throwError pure outcome
  where
    -- What opening a connection throws: the TCP connection, the request,
    -- the answer of the server.
    openingFailure err =
      or
        [ maybe False (\(_ :: IOException) -> True) (fromException err)
        , maybe False (\(_ :: WS.WebSocketError) -> True) (fromException err)
        , maybe False (\(_ :: HttpError) -> True) (fromException err)
        ]

-- | Ping at each interval, and fail when the pong does not come in time.
-- Stops quietly once nothing can be sent: the connection is then closed,
-- which the action sees for itself.
watch :: PingPong -> WS.Connection -> IO ()
watch pingPong connection = loop `catch` (\(_ :: WS.WebSocketError) -> pure ()) `catch` (\(_ :: IOException) -> pure ())
  where
    loop = forever $ do
      threadDelay (pingPong.interval * 1_000_000)
      before <- atomically (WS.pongCount connection)
      WS.ping connection ""
      expired <- registerDelay (pingPong.timeout * 1_000_000)
      answered <- atomically $ do
        count <- WS.pongCount connection
        late <- readTVar expired
        check (count > before || late)
        pure (count > before)
      unless answered (throwIO PongTimeout)

-- | Once the connection is known to be closed, nothing is sent.
sendTo :: IORef Bool -> WS.Connection -> Message -> IO Bool
sendTo closed connection message =
  readIORef closed >>= \case
    True -> pure False
    False ->
      ( True <$ WS.send connection (case message of
          TextMessage text -> WS.TextMessage text
          BinaryMessage bytes -> WS.BinaryMessage bytes)
      )
        `catch` (\(_ :: WS.WebSocketError) -> lost)
        `catch` (\(_ :: IOException) -> lost)
  where
    lost = False <$ writeIORef closed True

receiveFrom :: IORef Bool -> WS.Connection -> IO (Maybe Message)
receiveFrom closed connection =
  (WS.receive connection >>= maybe lost (pure . Just . fromMessage))
    `catch` (\(_ :: WS.WebSocketError) -> lost)
    `catch` (\(_ :: IOException) -> lost)
  where
    lost = Nothing <$ writeIORef closed True
    fromMessage = \case
      WS.TextMessage text -> TextMessage text
      WS.BinaryMessage bytes -> BinaryMessage bytes
