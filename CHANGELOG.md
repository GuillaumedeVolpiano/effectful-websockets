# Revision history for effectful-websockets

## 0.1.1.2 -- 2026-11-27

* Builds with GHC 9.8 and 9.10 as well as 9.12: the language is GHC2021
  with the extensions GHC2024 adds, and the bounds on `base` and on the
  libraries that come with the compiler are wider.
* Every exported name is documented.
* The links of the documentation say which name they mean, where a type
  and its constructor, or two modules, share one.

## 0.1.1.1 -- 2026-11-21

* Built on streamly-http 0.3 and streamly-websockets 0.3.

## 0.1.1.0 -- 2026-11-21

* `runWebSocketOver` and `connectionConfig`, for interpreters that open
  the connection another way: effectful-websockets-tls does, over TLS.
* Built on streamly-websockets 0.2: a connection that does not open
  within thirty seconds fails with `ConnectFailed`.
* The tests run against the server of streamly-websockets; the websockets
  package is no longer needed.

## 0.1.0.0 -- 2026-11-21

* `Effectful.WebSocket`: the `WebSocket` effect, with text and binary
  messages, a stream of received messages, and an interpreter from two
  actions.
* `Effectful.WebSocket.Client`: a client over `streamly-websockets`, with
  configurable pings, that keeps the action in the calling thread.
* `traced`, to observe the messages of a connection.
* The client waits for the server to answer the closing of the connection.
