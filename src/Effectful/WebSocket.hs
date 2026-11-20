-- |
-- Module      : Effectful.WebSocket
-- Description : A WebSocket connection as an effect
-- License     : BSD-3-Clause
--
-- Sending and receiving whole messages over an open WebSocket. The
-- connection itself is opened by an interpreter, such as
-- 'Effectful.WebSocket.Client.runWebSocketClient', or simulated by
-- 'runWebSocketWith' in tests.
--
-- Sending is safe from several threads at once: each message goes out
-- whole.
module Effectful.WebSocket
  ( -- * Effect
    WebSocket (..)
  , Message (..)
  , sendMessage
  , sendText
  , sendBinary
  , receiveMessage
  , messages

    -- * Interpreters
  , runWebSocketWith
  ) where

import Data.ByteString (ByteString)
import Data.Text (Text)
import Effectful
import Effectful.Dispatch.Dynamic (interpret_, send)
import Streamly.Data.Stream (Stream)
import Streamly.Data.Stream qualified as Stream

-- | A whole WebSocket message.
data Message
  = TextMessage Text
  | BinaryMessage ByteString
  deriving stock (Eq, Show)

-- | Exchange messages over an open WebSocket.
data WebSocket :: Effect where
  SendMessage :: Message -> WebSocket m Bool
  ReceiveMessage :: WebSocket m (Maybe Message)

type instance DispatchOf WebSocket = Dynamic

-- | Send a message. 'False' means the connection is closed and the
-- message was not sent.
sendMessage :: WebSocket :> es => Message -> Eff es Bool
sendMessage = send . SendMessage

sendText :: WebSocket :> es => Text -> Eff es Bool
sendText = sendMessage . TextMessage

sendBinary :: WebSocket :> es => ByteString -> Eff es Bool
sendBinary = sendMessage . BinaryMessage

-- | Wait for the next message. 'Nothing' means the connection is closed.
receiveMessage :: WebSocket :> es => Eff es (Maybe Message)
receiveMessage = send ReceiveMessage

-- | The messages received until the connection closes.
messages :: WebSocket :> es => Stream (Eff es) Message
messages = Stream.unfoldrM (const (fmap (,()) <$> receiveMessage)) ()

-- | Interpret the effect with two actions: for tests, or for another
-- transport.
runWebSocketWith
  :: (Message -> Eff es Bool)
  -- ^ Send a message; 'False' when the connection is closed.
  -> Eff es (Maybe Message)
  -- ^ Receive a message; 'Nothing' when the connection is closed.
  -> Eff (WebSocket : es) a
  -> Eff es a
runWebSocketWith sendAction receiveAction = interpret_ $ \case
  SendMessage message -> sendAction message
  ReceiveMessage -> receiveAction
