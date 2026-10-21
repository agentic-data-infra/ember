// User-defined functions are plain Velox simple functions, registered by
// name. Put them in a library that the driver and every worker link, and
// register them before building DataFrames that call them:
//
//   template <typename T>
//   struct TimesTwo {
//     VELOX_DEFINE_FUNCTION_TYPES(T);
//     void call(int64_t& out, const int64_t& in) { out = in * 2; }
//   };
//
//   ember::registerUdf<TimesTwo, int64_t, int64_t>("times_two");
//   df.select(call("times_two", col("x")));

#pragma once

#include <string>
#include <vector>

#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"

namespace ember {

template <template <class> class Func, typename TReturn, typename... TArgs>
void registerUdf(const std::vector<std::string>& names) {
  facebook::velox::registerFunction<Func, TReturn, TArgs...>(names);
}

template <template <class> class Func, typename TReturn, typename... TArgs>
void registerUdf(const std::string& name) {
  registerUdf<Func, TReturn, TArgs...>(std::vector<std::string>{name});
}

} // namespace ember
