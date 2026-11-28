# effectful-websockets

A WebSocket connection as an [Effectful](https://hackage.haskell.org/package/effectful)
effect.

- `Effectful.WebSocket`: the `WebSocket` effect. Send and receive whole
  text or binary messages, one at a time or as a
  [Streamly](https://hackage.haskell.org/package/streamly-core) stream
  (`messages`). `runWebSocketWith` interprets it with two actions, for
  tests; `traced` reports every message to an observer, whatever the
  interpreter.
- `Effectful.WebSocket.Client`: `runWebSocketClient` opens a connection
  with the
  [streamly-websockets](https://hackage.haskell.org/package/streamly-websockets)
  package and runs an action over it.

```haskell
{-# LANGUAGE OverloadedStrings #-}

import Effectful (Eff, IOE, (:>))
import Effectful.Error.Static (Error)
import Effectful.WebSocket (Message, receiveMessage, sendText)
import Effectful.WebSocket.Client (WebSocketError, clientConfig,
                                   runWebSocketClient)

hello :: (IOE :> es, Error WebSocketError :> es) => Eff es (Maybe Message)
hello = runWebSocketClient (clientConfig "127.0.0.1" 8080 "/") $ do
  _ <- sendText "hello"
  receiveMessage
```

## Behaviour

- The action runs in the calling thread, so effects with thread-local
  state keep its changes. The pings are sent by a watchdog thread.
- The server is pinged every ten seconds by default, with twenty seconds to
  answer; `NoPong` reports a silent server. Pongs are only seen while the
  action receives messages.
- Sending from several threads at once is safe: each message goes out
  whole.
- A connection closed by the server is not an error: receiving returns
  `Nothing`, and sending then returns `False`.
- When the action is done, the client closes the WebSocket and waits for the
  server to answer (one second by default) before the TCP connection goes,
  so that the server reads all that was sent.
- Plain TCP only. Secure WebSockets (`wss://`) are in
  [effectful-websockets-tls](https://github.com/GuillaumedeVolpiano/effectful-websockets-tls),
  a package apart, so that this one does not need a TLS stack.

A server that breaks the protocol, or sends a message larger than
`maxMessageSize`, ends the connection like a server that closes it.

To build against copies of
[streamly-websockets](https://hackage.haskell.org/package/streamly-websockets)
and [streamly-http](https://hackage.haskell.org/package/streamly-http)
checked out next to this repository, pass
`--project-file=cabal.project.siblings`.

Tested with GHC 9.8.4, 9.10.3 and 9.12.4, and effectful 2.7.1, against the
server of streamly-websockets.

## License

BSD-3-Clause. See [LICENSE](LICENSE).
