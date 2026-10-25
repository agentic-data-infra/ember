// Session config from the environment for the examples: $EMBER_CONF holds
// "key=value" pairs separated by spaces, e.g. to keep shuffle outputs in Kiln:
//
//   EMBER_CONF="ember.shuffle.store=kiln ember.shuffle.kiln.metadata=localhost:7470"

#pragma once

#include <cstdlib>
#include <sstream>

#include "ember/Session.h"

namespace ember::examples {

inline Session::Builder& withEnvConfig(Session::Builder& builder) {
  const char* conf = std::getenv("EMBER_CONF");
  std::istringstream pairs(conf == nullptr ? "" : conf);
  std::string pair;
  while (pairs >> pair) {
    const auto equals = pair.find('=');
    if (equals == std::string::npos) {
      throw std::invalid_argument("EMBER_CONF entry without '=': " + pair);
    }
    builder.config(pair.substr(0, equals), pair.substr(equals + 1));
  }
  return builder;
}

} // namespace ember::examples
