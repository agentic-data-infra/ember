// ember._ember: the Python binding of Ember's Dataframe API (ember/api).
//
// A thin layer: each Python method calls the C++ method of the same name.
// Results come back as Arrow data through Velox's Arrow bridge, so Python
// gets pyarrow tables; the python/ember package wraps this module.
//
// Python values stand in for columns the way they do in the C++ API: in
// select(), groupBy() and the functions, a str is a column name; in
// operators (col("x") + 1, col("w") != "") a str or number is a literal.

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <unordered_map>

#include "ember/Session.h"
#include "ember/functions.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/arrow/Abi.h"
#include "velox/vector/arrow/Bridge.h"

namespace py = pybind11;
namespace velox = facebook::velox;

using namespace ember;

namespace {

// Pool for the Arrow export and import of results and inputs. Exported
// arrays may outlive any session, so it lives as long as the process.
velox::memory::MemoryPool* arrowPool() {
  static auto pool = velox::memory::memoryManager()->addLeafPool("ember_python");
  return pool.get();
}

Column literal(py::handle value) {
  if (py::isinstance<Column>(value)) {
    return value.cast<Column>();
  }
  // bool before int: Python bools are ints.
  if (py::isinstance<py::bool_>(value)) {
    return lit(value.cast<bool>());
  }
  if (py::isinstance<py::int_>(value)) {
    return lit(value.cast<int64_t>());
  }
  if (py::isinstance<py::float_>(value)) {
    return lit(value.cast<double>());
  }
  if (py::isinstance<py::str>(value)) {
    return lit(value.cast<std::string>());
  }
  throw py::type_error(
      "expected a Column, bool, int, float or str, got " +
      std::string(py::str(value.get_type())));
}

// A column argument: a Column, or a str naming one.
Column column(py::handle value) {
  if (py::isinstance<py::str>(value)) {
    return Column(value.cast<std::string>());
  }
  if (py::isinstance<Column>(value)) {
    return value.cast<Column>();
  }
  throw py::type_error(
      "expected a Column or a column name, got " +
      std::string(py::str(value.get_type())));
}

// Columns from varargs, where a list or tuple stands for its items, so both
// select("a", "b") and select(["a", "b"]) work.
std::vector<Column> columns(const py::args& args) {
  std::vector<Column> result;
  for (auto arg : args) {
    if (py::isinstance<py::list>(arg) || py::isinstance<py::tuple>(arg)) {
      for (auto item : arg) {
        result.push_back(column(item));
      }
    } else {
      result.push_back(column(arg));
    }
  }
  return result;
}

std::vector<std::string> strings(const py::args& args) {
  std::vector<std::string> result;
  for (auto arg : args) {
    if (py::isinstance<py::list>(arg) || py::isinstance<py::tuple>(arg)) {
      for (auto item : arg) {
        result.push_back(item.cast<std::string>());
      }
    } else {
      result.push_back(arg.cast<std::string>());
    }
  }
  return result;
}

velox::TypePtr parseType(const std::string& name) {
  static const std::unordered_map<std::string, velox::TypePtr> types{
      {"boolean", velox::BOOLEAN()},
      {"tinyint", velox::TINYINT()},
      {"byte", velox::TINYINT()},
      {"smallint", velox::SMALLINT()},
      {"short", velox::SMALLINT()},
      {"int", velox::INTEGER()},
      {"integer", velox::INTEGER()},
      {"bigint", velox::BIGINT()},
      {"long", velox::BIGINT()},
      {"real", velox::REAL()},
      {"float", velox::REAL()},
      {"double", velox::DOUBLE()},
      {"string", velox::VARCHAR()},
      {"varchar", velox::VARCHAR()},
      {"date", velox::DATE()},
  };
  std::string lower = name;
  std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
  auto it = types.find(lower);
  if (it == types.end()) {
    throw py::value_error("unknown type: " + name);
  }
  return it->second;
}

// Result batches as a pyarrow Table with schema 'schema'.
py::object toArrow(
    const std::vector<velox::RowVectorPtr>& batches,
    const velox::RowTypePtr& schema) {
  auto pa = py::module_::import("pyarrow");
  auto recordBatch = pa.attr("RecordBatch");
  py::list pyBatches;
  for (const auto& batch : batches) {
    ArrowArray array;
    ArrowSchema arrowSchema;
    velox::exportToArrow(batch, array, arrowPool());
    velox::exportToArrow(batch, arrowSchema);
    // pyarrow takes ownership of both and releases them.
    pyBatches.append(recordBatch.attr("_import_from_c")(
        reinterpret_cast<uintptr_t>(&array),
        reinterpret_cast<uintptr_t>(&arrowSchema)));
  }
  if (!batches.empty()) {
    return pa.attr("Table").attr("from_batches")(pyBatches);
  }
  // No rows: export an empty vector for the schema.
  auto empty = velox::BaseVector::create(schema, 0, arrowPool());
  ArrowSchema arrowSchema;
  velox::exportToArrow(empty, arrowSchema);
  auto paSchema = pa.attr("Schema").attr("_import_from_c")(
      reinterpret_cast<uintptr_t>(&arrowSchema));
  return pa.attr("Table").attr("from_batches")(pyBatches, paSchema);
}

// A pyarrow Table (or RecordBatch) as Velox row vectors.
std::vector<velox::RowVectorPtr> fromArrow(py::handle data) {
  auto pa = py::module_::import("pyarrow");
  py::object batches = py::isinstance(data, pa.attr("RecordBatch"))
      ? py::object(py::make_tuple(data))
      : data.attr("to_batches")();
  std::vector<velox::RowVectorPtr> result;
  for (auto batch : batches) {
    ArrowArray array;
    ArrowSchema schema;
    batch.attr("_export_to_c")(
        reinterpret_cast<uintptr_t>(&array),
        reinterpret_cast<uintptr_t>(&schema));
    auto vector = velox::importFromArrowAsOwner(schema, array, arrowPool());
    result.push_back(std::dynamic_pointer_cast<velox::RowVector>(vector));
  }
  return result;
}

std::vector<std::pair<std::string, std::string>> dtypes(const Dataframe& df) {
  auto schema = df.schema();
  std::vector<std::pair<std::string, std::string>> result;
  for (size_t i = 0; i < schema->size(); ++i) {
    result.emplace_back(schema->nameOf(i), schema->childAt(i)->toString());
  }
  return result;
}

} // namespace

PYBIND11_MODULE(_ember, m) {
  m.doc() = "Ember's Dataframe API (C++), bound for Python.";

  // Column.

  auto columnClass = py::class_<Column>(m, "Column");
  auto binary = [&](const char* name, Column (*op)(const Column&, const Column&)) {
    columnClass.def(name, [op](const Column& self, py::handle other) {
      return op(self, literal(other));
    });
  };
  auto reflected = [&](const char* name, Column (*op)(const Column&, const Column&)) {
    columnClass.def(name, [op](const Column& self, py::handle other) {
      return op(literal(other), self);
    });
  };
#define EMBER_OP(name, op)                                           \
  binary(name, [](const Column& a, const Column& b) { return a op b; });
#define EMBER_ROP(name, op)                                          \
  reflected(name, [](const Column& a, const Column& b) { return a op b; });
  EMBER_OP("__add__", +)
  EMBER_OP("__sub__", -)
  EMBER_OP("__mul__", *)
  EMBER_OP("__truediv__", /)
  EMBER_OP("__mod__", %)
  EMBER_OP("__eq__", ==)
  EMBER_OP("__ne__", !=)
  EMBER_OP("__lt__", <)
  EMBER_OP("__le__", <=)
  EMBER_OP("__gt__", >)
  EMBER_OP("__ge__", >=)
  EMBER_OP("__and__", &&)
  EMBER_OP("__or__", ||)
  EMBER_ROP("__radd__", +)
  EMBER_ROP("__rsub__", -)
  EMBER_ROP("__rmul__", *)
  EMBER_ROP("__rtruediv__", /)
  EMBER_ROP("__rmod__", %)
  EMBER_ROP("__rand__", &&)
  EMBER_ROP("__ror__", ||)
#undef EMBER_OP
#undef EMBER_ROP

  columnClass
      .def("__invert__", [](const Column& c) { return !c; })
      .def("__neg__", [](const Column& c) { return -c; })
      .def(
          "__bool__",
          [](const Column&) -> bool {
            throw py::value_error(
                "a Column has no truth value: use & for 'and', | for 'or', "
                "~ for 'not'");
          })
      .def(
          "alias",
          [](const Column& c, std::string alias) { return c.as(std::move(alias)); },
          py::arg("alias"))
      .def(
          "name",
          [](const Column& c, std::string alias) { return c.as(std::move(alias)); },
          py::arg("alias"))
      .def("asc", &Column::asc)
      .def("asc_nulls_first", &Column::asc_nulls_first)
      .def("asc_nulls_last", &Column::asc_nulls_last)
      .def("desc", &Column::desc)
      .def("desc_nulls_first", &Column::desc_nulls_first)
      .def("desc_nulls_last", &Column::desc_nulls_last)
      .def(
          "cast",
          [](const Column& c, const std::string& type) {
            return c.cast(parseType(type));
          },
          py::arg("type"))
      .def("isNull", &Column::isNull)
      .def("isNotNull", &Column::isNotNull)
      .def(
          "isin",
          [](const Column& c, const py::args& values) {
            std::vector<Column> literals;
            for (auto value : values) {
              literals.push_back(literal(value));
            }
            return c.isin(literals);
          })
      .def("__str__", &Column::toString)
      .def("__repr__", [](const Column& c) {
        return "Column<'" + c.toString() + "'>";
      });
  // __eq__ returns a Column, so Columns are not hashable.
  columnClass.attr("__hash__") = py::none();

  // Dataframe.

  py::class_<GroupedData>(m, "GroupedData")
      .def("agg", [](const GroupedData& g, const py::args& aggs) {
        return g.agg(columns(aggs));
      })
      .def("count", &GroupedData::count)
      .def("sum", [](const GroupedData& g, const py::args& names) {
        return g.sum(strings(names));
      })
      .def("avg", [](const GroupedData& g, const py::args& names) {
        return g.avg(strings(names));
      })
      .def("min", [](const GroupedData& g, const py::args& names) {
        return g.min(strings(names));
      })
      .def("max", [](const GroupedData& g, const py::args& names) {
        return g.max(strings(names));
      });

  py::class_<Dataframe>(m, "Dataframe")
      .def("select", [](const Dataframe& df, const py::args& cols) {
        return df.select(columns(cols));
      })
      .def("selectExpr", [](const Dataframe& df, const py::args& exprs) {
        return df.selectExpr(strings(exprs));
      })
      .def(
          "filter",
          [](const Dataframe& df, py::handle condition) {
            if (py::isinstance<py::str>(condition)) {
              return df.filter(condition.cast<std::string>());
            }
            return df.filter(condition.cast<Column>());
          },
          py::arg("condition"))
      .def(
          "where",
          [](const Dataframe& df, py::handle condition) {
            if (py::isinstance<py::str>(condition)) {
              return df.where(condition.cast<std::string>());
            }
            return df.where(condition.cast<Column>());
          },
          py::arg("condition"))
      .def(
          "withColumn",
          [](const Dataframe& df, const std::string& name, py::handle col) {
            return df.withColumn(name, literal(col));
          },
          py::arg("name"),
          py::arg("col"))
      .def(
          "withColumnRenamed",
          &Dataframe::withColumnRenamed,
          py::arg("existing"),
          py::arg("new"))
      .def("drop", [](const Dataframe& df, const py::args& names) {
        return df.drop(strings(names));
      })
      .def("groupBy", [](const Dataframe& df, const py::args& cols) {
        return df.groupBy(columns(cols));
      })
      .def("agg", [](const Dataframe& df, const py::args& aggs) {
        return df.agg(columns(aggs));
      })
      .def("orderBy", [](const Dataframe& df, const py::args& cols) {
        return df.orderBy(columns(cols));
      })
      .def("sort", [](const Dataframe& df, const py::args& cols) {
        return df.sort(columns(cols));
      })
      .def("limit", &Dataframe::limit, py::arg("n"))
      .def("distinct", &Dataframe::distinct)
      .def("unionAll", &Dataframe::unionAll, py::arg("other"))
      .def("union", &Dataframe::unionAll, py::arg("other"))
      .def(
          "join",
          [](const Dataframe& df,
             const Dataframe& other,
             py::handle on,
             const std::string& how) {
            std::vector<std::string> using_;
            if (py::isinstance<py::str>(on)) {
              using_.push_back(on.cast<std::string>());
            } else {
              using_ = on.cast<std::vector<std::string>>();
            }
            return df.join(other, using_, how);
          },
          py::arg("other"),
          py::arg("on"),
          py::arg("how") = "inner")
      .def("cache", &Dataframe::cache)
      .def("persist", &Dataframe::persist)
      .def(
          "unpersist",
          [](const Dataframe& df) { return df.unpersist(); })
      // Actions run without the GIL: Ember executes on its own threads.
      .def(
          "toArrow",
          [](const Dataframe& df) {
            std::vector<velox::RowVectorPtr> batches;
            {
              py::gil_scoped_release release;
              batches = df.collectBatches();
            }
            return toArrow(batches, df.schema());
          },
          "The result as a pyarrow.Table.")
      .def("count", &Dataframe::count, py::call_guard<py::gil_scoped_release>())
      .def(
          "showString",
          &Dataframe::showString,
          py::arg("n") = 20,
          py::arg("truncate") = true,
          py::call_guard<py::gil_scoped_release>())
      .def(
          "explainString",
          &Dataframe::explainString,
          py::call_guard<py::gil_scoped_release>())
      .def_property_readonly("columns", &Dataframe::columns)
      .def_property_readonly("dtypes", &dtypes)
      .def("schemaString", [](const Dataframe& df) {
        return df.schema()->toString();
      });

  // Session.

  py::class_<DataframeReader>(m, "DataframeReader")
      .def("text", &DataframeReader::text, py::arg("path"))
      .def("parquet", &DataframeReader::parquet, py::arg("path"))
      .def(
          "format",
          &DataframeReader::format,
          py::arg("source"),
          py::return_value_policy::reference_internal)
      .def("load", &DataframeReader::load, py::arg("path"));

  py::class_<Session::Builder>(m, "SessionBuilder")
      .def(
          "appName",
          &Session::Builder::appName,
          py::arg("name"),
          py::return_value_policy::reference_internal)
      .def(
          "master",
          &Session::Builder::master,
          py::arg("master"),
          py::return_value_policy::reference_internal)
      .def(
          "config",
          [](Session::Builder& b, const std::string& key, py::handle value)
              -> Session::Builder& {
            // PySpark style: config("k", 4) and config("k", True) work too.
            std::string s = py::isinstance<py::bool_>(value)
                ? (value.cast<bool>() ? "true" : "false")
                : std::string(py::str(value));
            return b.config(key, s);
          },
          py::arg("key"),
          py::arg("value"),
          py::return_value_policy::reference_internal)
      .def(
          "getOrCreate",
          &Session::Builder::getOrCreate,
          py::call_guard<py::gil_scoped_release>());

  py::class_<Session>(m, "Session")
      .def_property_readonly_static(
          "builder", [](py::object) { return Session::builder(); })
      .def_static("active", &Session::active)
      .def_property_readonly("read", &Session::read)
      .def(
          "createDataFrame",
          [](const Session& s, py::handle data) {
            return s.createDataframe(fromArrow(data));
          },
          py::arg("data"),
          "A Dataframe over a pyarrow.Table or RecordBatch.")
      .def_property_readonly(
          "appName", [](const Session& s) { return s.state().appName(); })
      .def_property_readonly(
          "master", [](const Session& s) { return s.state().master(); })
      .def_property_readonly(
          "parallelism",
          [](const Session& s) { return s.state().parallelism(); })
      .def("stop", &Session::stop, py::call_guard<py::gil_scoped_release>());

  // functions.

  namespace f = ember::functions;
  auto fn = m.def_submodule("functions", "Column functions.");
  fn.def("col", [](const std::string& name) { return f::col(name); })
      .def("column", [](const std::string& name) { return f::column(name); })
      .def("lit", [](py::handle value) { return literal(value); })
      .def("expr", [](const std::string& sql) { return f::expr(sql); })
      .def(
          "call",
          [](const std::string& name, const py::args& args) {
            return f::call(name, columns(args));
          },
          "Calls the registered Velox function 'name'.")
      .def("explode", [](py::handle c) { return f::explode(column(c)); })
      .def(
          "split",
          [](py::handle c, const std::string& pattern, std::optional<int32_t> limit) {
            return limit ? f::split(column(c), pattern, *limit)
                         : f::split(column(c), pattern);
          },
          py::arg("str"),
          py::arg("pattern"),
          py::arg("limit") = py::none())
      .def("lower", [](py::handle c) { return f::lower(column(c)); })
      .def("upper", [](py::handle c) { return f::upper(column(c)); })
      .def("length", [](py::handle c) { return f::length(column(c)); })
      .def("trim", [](py::handle c) { return f::trim(column(c)); })
      .def("ltrim", [](py::handle c) { return f::ltrim(column(c)); })
      .def("rtrim", [](py::handle c) { return f::rtrim(column(c)); })
      .def("concat", [](const py::args& cols) { return f::concat(columns(cols)); })
      .def(
          "substring",
          [](py::handle c, int32_t pos, int32_t len) {
            return f::substring(column(c), pos, len);
          },
          py::arg("str"),
          py::arg("pos"),
          py::arg("len"))
      .def(
          "regexp_replace",
          [](py::handle c, const std::string& pattern, const std::string& replacement) {
            return f::regexp_replace(column(c), pattern, replacement);
          },
          py::arg("str"),
          py::arg("pattern"),
          py::arg("replacement"))
      .def(
          "regexp_extract",
          [](py::handle c, const std::string& pattern, int32_t group) {
            return f::regexp_extract(column(c), pattern, group);
          },
          py::arg("str"),
          py::arg("pattern"),
          py::arg("idx"))
      .def("size", [](py::handle c) { return f::size(column(c)); })
      .def(
          "array_contains",
          [](py::handle c, py::handle value) {
            return f::array_contains(column(c), literal(value));
          },
          py::arg("col"),
          py::arg("value"))
      .def("abs", [](py::handle c) { return f::abs(column(c)); })
      .def(
          "round",
          [](py::handle c, int32_t scale) { return f::round(column(c), scale); },
          py::arg("col"),
          py::arg("scale") = 0)
      .def("coalesce", [](const py::args& cols) { return f::coalesce(columns(cols)); })
      .def("count", [](py::handle c) { return f::count(column(c)); })
      .def("countAll", &f::countAll)
      .def("countDistinct", [](py::handle c) { return f::countDistinct(column(c)); })
      .def("sum", [](py::handle c) { return f::sum(column(c)); })
      .def("avg", [](py::handle c) { return f::avg(column(c)); })
      .def("mean", [](py::handle c) { return f::mean(column(c)); })
      .def("min", [](py::handle c) { return f::min(column(c)); })
      .def("max", [](py::handle c) { return f::max(column(c)); })
      .def("first", [](py::handle c) { return f::first(column(c)); })
      .def("collect_list", [](py::handle c) { return f::collect_list(column(c)); });
}
