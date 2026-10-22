// Column functions. Each one is a thin builder over a Velox function call;
// Ember registers Velox's sparksql function package for these semantics
// (e.g. split takes a regex).
// call() reaches any registered Velox function, including UDFs.

#pragma once

#include <string>
#include <vector>

#include "ember/Column.h"

namespace ember::functions {

using ember::lit;

Column col(const std::string& name);
Column column(const std::string& name);

/// A SQL expression string, e.g. expr("lower(value) AS v").
Column expr(const std::string& sql);

/// Calls the registered function 'name', e.g. a UDF.
Column call(const std::string& name, const std::vector<Column>& args);

template <typename... T>
Column call(const std::string& name, T... args) {
  return call(name, std::vector<Column>{Column(args)...});
}

// Generators.
Column explode(const Column& array);

// Strings.
Column split(const Column& str, const std::string& pattern);
Column split(const Column& str, const std::string& pattern, int32_t limit);
Column lower(const Column& str);
Column upper(const Column& str);
Column length(const Column& str);
Column trim(const Column& str);
Column ltrim(const Column& str);
Column rtrim(const Column& str);
Column concat(const std::vector<Column>& columns);
Column substring(const Column& str, int32_t pos, int32_t len);
Column regexp_replace(
    const Column& str,
    const std::string& pattern,
    const std::string& replacement);
/// The 'group'th capture group of the first match of 'pattern' in 'str'
/// (0 = the whole match); "" when nothing matches.
Column regexp_extract(
    const Column& str,
    const std::string& pattern,
    int32_t group);

// Collections.
Column size(const Column& collection);
Column array_contains(const Column& array, const Column& value);

// Math.
Column abs(const Column& value);
/// Rounds half up to 'scale' decimal places.
Column round(const Column& value, int32_t scale = 0);

// Conditionals.
Column coalesce(const std::vector<Column>& columns);

// Aggregates.
Column count(const Column& value);
/// count(*); count(col("*")) means the same.
Column countAll();
Column countDistinct(const Column& value);
Column sum(const Column& value);
Column avg(const Column& value);
Column mean(const Column& value);
Column min(const Column& value);
Column max(const Column& value);
Column first(const Column& value);
Column collect_list(const Column& value);

} // namespace ember::functions
