# Revision history for effectful-websockets

## 0.1.0.0 -- 2026-11-28

* First release on Hackage.
* `Effectful.WebSocket`: the `WebSocket` effect, with text and binary
  messages, a stream of received messages, and an interpreter from two
  actions; `traced`, to observe the messages of a connection.
* `Effectful.WebSocket.Client`: a client over streamly-websockets, with
  configurable pings, that keeps the action in the calling thread and
  waits for the server to answer the closing of the connection.
* `runWebSocketOver` and `connectionConfig`, for interpreters that open
  the connection another way, as effectful-websockets-tls does.
