#include "ember/worker/TaskDescription.h"

#include <folly/io/Cursor.h>
#include <folly/json.h>
#include <folly/lang/Bits.h>

#include "velox/common/base/Exceptions.h"

namespace ember {

std::string TaskDescription::taskId() const {
  return fmt::format(
      "ember://{}/{}.{}/{}.{}",
      jobId,
      stageId,
      stageAttempt,
      partition,
      attempt);
}

std::string TaskDescription::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["jobId"] = jobId;
  obj["stageId"] = stageId;
  obj["stageAttempt"] = stageAttempt;
  obj["partition"] = partition;
  obj["attempt"] = attempt;
  obj["fragment"] = fragment;
  folly::dynamic splitsObj = folly::dynamic::object;
  for (const auto& [nodeId, nodeSplits] : splits) {
    folly::dynamic list = folly::dynamic::array;
    for (const auto& split : nodeSplits) {
      list.push_back(split);
    }
    splitsObj[nodeId] = std::move(list);
  }
  obj["splits"] = std::move(splitsObj);
  obj["resultTask"] = resultTask;
  folly::dynamic configObj = folly::dynamic::object;
  for (const auto& [key, value] : config) {
    configObj[key] = value;
  }
  obj["config"] = std::move(configObj);
  return folly::toJson(obj);
}

namespace {

// Plans of long Dataframe chains nest deeper than folly's default limit.
folly::dynamic parseJson(std::string_view json) {
  folly::json::serialization_opts options;
  options.recursion_limit = 100'000;
  return folly::parseJson(json, options);
}

} // namespace

TaskDescription TaskDescription::deserialize(const std::string& bytes) {
  auto obj = parseJson(bytes);
  TaskDescription task;
  task.jobId = obj["jobId"].asString();
  task.stageId = obj["stageId"].asInt();
  task.stageAttempt = obj["stageAttempt"].asInt();
  task.partition = obj["partition"].asInt();
  task.attempt = obj["attempt"].asInt();
  task.fragment = obj["fragment"];
  for (const auto& [nodeId, list] : obj["splits"].items()) {
    auto& nodeSplits = task.splits[nodeId.asString()];
    for (const auto& split : list) {
      nodeSplits.push_back(split);
    }
  }
  task.resultTask = obj["resultTask"].asBool();
  for (const auto& [key, value] : obj["config"].items()) {
    task.config[key.asString()] = value.asString();
  }
  return task;
}

// A result is a JSON header followed by the result pages, each as a uint32
// length and its bytes.
std::string TaskResult::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["taskId"] = taskId;
  obj["state"] = state == TaskState::kFinished ? "finished" : "failed";
  obj["error"] = error;
  obj["userError"] = userError;
  if (fetchFailure.has_value()) {
    obj["fetchFailure"] =
        shuffle::blockUrl(fetchFailure->location, fetchFailure->block);
  }
  if (mapStatus.has_value()) {
    folly::dynamic lengths = folly::dynamic::array;
    for (auto length : mapStatus->partitionLengths) {
      lengths.push_back(length);
    }
    obj["mapStatus"] = folly::dynamic::object("location", mapStatus->location)(
        "partitionLengths", std::move(lengths));
  }
  obj["numPages"] = resultPages.size();
  obj["outputRows"] = outputRows;
  obj["wallMicros"] = wallMicros;
  auto header = folly::toJson(obj);

  std::string out;
  auto appendLength = [&](size_t length) {
    VELOX_CHECK_LE(length, std::numeric_limits<uint32_t>::max());
    const auto value = folly::Endian::little(static_cast<uint32_t>(length));
    out.append(reinterpret_cast<const char*>(&value), sizeof(value));
  };
  appendLength(header.size());
  out.append(header);
  for (const auto& page : resultPages) {
    appendLength(page.size());
    out.append(page);
  }
  return out;
}

TaskResult TaskResult::deserialize(const std::string& bytes) {
  auto buffer = folly::IOBuf::wrapBufferAsValue(bytes.data(), bytes.size());
  folly::io::Cursor cursor(&buffer);
  auto obj = parseJson(cursor.readFixedString(cursor.readLE<uint32_t>()));
  TaskResult result;
  result.taskId = obj["taskId"].asString();
  result.state = obj["state"].asString() == "finished" ? TaskState::kFinished
                                                       : TaskState::kFailed;
  result.error = obj["error"].asString();
  result.userError = obj["userError"].asBool();
  if (obj.count("fetchFailure")) {
    auto [location, block] =
        shuffle::parseBlockUrl(obj["fetchFailure"].asString());
    result.fetchFailure = shuffle::FetchFailure{std::move(location), block};
  }
  if (obj.count("mapStatus")) {
    shuffle::MapStatus status;
    status.location = obj["mapStatus"]["location"].asString();
    for (const auto& length : obj["mapStatus"]["partitionLengths"]) {
      status.partitionLengths.push_back(length.asInt());
    }
    result.mapStatus = std::move(status);
  }
  const auto numPages = obj["numPages"].asInt();
  for (int64_t i = 0; i < numPages; ++i) {
    result.resultPages.push_back(
        cursor.readFixedString(cursor.readLE<uint32_t>()));
  }
  result.outputRows = obj["outputRows"].asInt();
  result.wallMicros = obj["wallMicros"].asInt();
  return result;
}

} // namespace ember
