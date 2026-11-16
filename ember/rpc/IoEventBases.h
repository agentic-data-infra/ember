// The process's network IO threads: a small pool of event bases shared by
// RPC connections and other network clients (e.g. the HTTP client of the AI
// functions), so that no client starts threads of its own.

#pragma once

#include <folly/io/async/EventBase.h>

namespace ember::rpc {

/// An event base of the process's IO pool, chosen round robin. The pool lives
/// as long as the process.
folly::EventBase* nextIoEventBase();

} // namespace ember::rpc
