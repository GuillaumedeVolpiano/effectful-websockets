# Revision history for effectful-websockets

## 0.1.0.0 -- unreleased

* `Effectful.WebSocket`: the `WebSocket` effect, with text and binary
  messages, a stream of received messages, and an interpreter from two
  actions.
* `Effectful.WebSocket.Client`: a client over `websockets`, with
  configurable pings, that keeps the action in the calling thread.
