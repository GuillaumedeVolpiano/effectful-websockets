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
  with the [websockets](https://hackage.haskell.org/package/websockets)
  package and runs an action over it.

```haskell
import Effectful.WebSocket
import Effectful.WebSocket.Client

hello :: (IOE :> es, Error WebSocketError :> es) => Eff es (Maybe Message)
hello = runWebSocketClient (clientConfig "127.0.0.1" 8080 "/") $ do
  _ <- sendText "hello"
  receiveMessage
```

## Behaviour

- The action runs in the calling thread, so effects with thread-local
  state keep its changes. (`websockets` runs the action given to its ping
  helper in another thread.)
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
- Plain TCP only: no TLS (`wss://`).

Tested with GHC 9.12.4, effectful 2.7.1, websockets 0.13.

## License

BSD-3-Clause. See [LICENSE](LICENSE).
