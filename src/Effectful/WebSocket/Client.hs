-- |
-- Module      : Effectful.WebSocket.Client
-- Description : Opening a WebSocket to a server
-- License     : BSD-3-Clause
--
-- A client over the @websockets@ package, on plain TCP (no TLS).
--
-- The action runs in the thread of the caller, so that effects with
-- thread-local state, such as a local @State@, keep the changes it makes.
-- (@websockets@ runs the action given to its ping helper in a thread of its
-- own; this interpreter keeps the pings in a watchdog thread instead.)
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
  ) where

import Control.Concurrent.Async (ExceptionInLinkedThread (..), link, withAsync)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (IOException, SomeException, catch, displayException, finally, fromException, throwIO, try)
import Control.Monad (forever, void)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as LazyByteString
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding (decodeUtf8Lenient)
import Data.Text.Lazy qualified as LazyText
import Effectful
import Effectful.Error.Static (Error, throwError)
import Effectful.WebSocket (Message (..), WebSocket, runWebSocketWith)
import Network.WebSockets qualified as WS
import Network.WebSockets.Connection.PingPong (PongTimeout (..))
import System.Timeout (timeout)

-- | Where to connect, and how.
data ClientConfig = ClientConfig
  { host :: String
  , port :: Int
  , path :: String
  -- ^ Such as @\/api@.
  , headers :: WS.Headers
  -- ^ Extra headers for the opening handshake.
  , options :: WS.ConnectionOptions
  , pingPong :: Maybe PingPong
  -- ^ 'Nothing' sends no ping.
  , closeTimeout :: Int
  -- ^ Microseconds to wait, once the action is done, for the server to
  -- answer the closing of the connection.
  }

-- | A configuration with no extra header, the default options of
-- @websockets@, 'defaultPingPong', and one second to close.
clientConfig :: String -> Int -> String -> ClientConfig
clientConfig host port path =
  ClientConfig
    { host
    , port
    , path
    , headers = []
    , options = WS.defaultConnectionOptions
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

-- | Open a connection and run an action over it. The connection is closed
-- when the action returns.
--
-- Pongs are only seen while the action receives messages: an action that
-- stops receiving for longer than the timeout loses the connection.
--
-- Throws 'ConnectFailed' if the connection cannot be opened, and 'NoPong'
-- if the server stops answering pings. A connection closed by the server
-- is not an error: receiving returns 'Nothing' and sending 'False'.
runWebSocketClient
  :: (IOE :> es, Error WebSocketError :> es)
  => ClientConfig
  -> Eff (WebSocket : es) a
  -> Eff es a
runWebSocketClient config action = do
  outcome <- withEffToIO SeqUnlift $ \runInIO -> do
    connected <- newIORef False
    attempt <- try . WS.runClientWith config.host config.port config.path config.options config.headers $ \connection -> do
      writeIORef connected True
      closed <- newIORef False
      let run = runInIO (runWebSocketWith (liftIO . sendTo closed connection) (liftIO (receiveFrom closed connection)) action)
      value <- case config.pingPong of
        Nothing -> run
        Just pingPong -> do
          finished <- newEmptyMVar
          let watch = WS.withPingPong (pingPongOptions pingPong) connection (const (takeMVar finished))
          withAsync watch $ \watchdog -> link watchdog >> (run `finally` putMVar finished ())
      closeGracefully connection
      pure value
    wasConnected <- readIORef connected
    case attempt of
      Right value -> pure (Right value)
      Left (err :: SomeException)
        | not wasConnected -> pure (Left (ConnectFailed (Text.pack (displayException err))))
        | Just (ExceptionInLinkedThread _ cause) <- fromException err
        , Just PongTimeout <- fromException cause ->
            pure (Left NoPong)
        | otherwise -> throwIO err
  either throwError pure outcome
  where
    -- Send the close frame, then wait for the server's before the TCP
    -- connection goes: closing it at once can make the system reset it,
    -- and the server lose what it had not read yet.
    closeGracefully connection =
      ( do
          WS.sendClose connection ("" :: ByteString)
          void (timeout config.closeTimeout (forever (WS.receiveDataMessage connection)))
      )
        `catch` \(_ :: SomeException) -> pure ()
    pingPongOptions pingPong = WS.defaultPingPongOptions {WS.pingInterval = pingPong.interval, WS.pongTimeout = pingPong.timeout}

-- | Once the connection is known to be closed, nothing is sent: the
-- socket may still accept bytes after the server has closed the
-- WebSocket.
sendTo :: IORef Bool -> WS.Connection -> Message -> IO Bool
sendTo closed connection message =
  readIORef closed >>= \case
    True -> pure False
    False ->
      ( True <$ case message of
          TextMessage text -> WS.sendTextData connection text
          BinaryMessage bytes -> WS.sendBinaryData connection bytes
      )
        `catch` (\(_ :: WS.ConnectionException) -> lost)
        `catch` (\(_ :: IOException) -> lost)
  where
    lost = False <$ writeIORef closed True

receiveFrom :: IORef Bool -> WS.Connection -> IO (Maybe Message)
receiveFrom closed connection =
  (Just . fromDataMessage <$> WS.receiveDataMessage connection)
    `catch` (\(_ :: WS.ConnectionException) -> lost)
    `catch` (\(_ :: IOException) -> lost)
  where
    lost = Nothing <$ writeIORef closed True
    fromDataMessage = \case
      WS.Text _ (Just text) -> TextMessage (LazyText.toStrict text)
      WS.Text bytes Nothing -> TextMessage (decodeUtf8Lenient (LazyByteString.toStrict bytes))
      WS.Binary bytes -> BinaryMessage (LazyByteString.toStrict bytes)
