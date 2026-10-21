#include "ember/rpc/Messages.h"

#include <folly/dynamic.h>
#include <folly/json.h>

#include "ember/rpc/Rpc.h"

namespace ember::rpc {

namespace {

folly::dynamic parse(const std::string& bytes, const char* message) {
  try {
    auto obj = folly::parseJson(bytes);
    if (!obj.isObject()) {
      throw RpcError(fmt::format("Malformed {}", message));
    }
    return obj;
  } catch (const std::exception& e) {
    throw RpcError(fmt::format("Malformed {}: {}", message, e.what()));
  }
}

} // namespace

std::string RegisterWorker::serialize() const {
  return folly::toJson(folly::dynamic::object("workerId", workerId)(
      "address", address)("slots", slots));
}

RegisterWorker RegisterWorker::deserialize(const std::string& bytes) {
  auto obj = parse(bytes, "RegisterWorker");
  return RegisterWorker{
      obj["workerId"].asString(),
      obj["address"].asString(),
      static_cast<int32_t>(obj["slots"].asInt())};
}

std::string RegisterWorkerReply::serialize() const {
  return folly::toJson(folly::dynamic::object("appName", appName)(
      "heartbeatIntervalMs", heartbeatIntervalMs));
}

RegisterWorkerReply RegisterWorkerReply::deserialize(const std::string& bytes) {
  auto obj = parse(bytes, "RegisterWorkerReply");
  return RegisterWorkerReply{
      obj["appName"].asString(), obj["heartbeatIntervalMs"].asInt()};
}

std::string Heartbeat::serialize() const {
  return folly::toJson(folly::dynamic::object("workerId", workerId));
}

Heartbeat Heartbeat::deserialize(const std::string& bytes) {
  return Heartbeat{parse(bytes, "Heartbeat")["workerId"].asString()};
}

std::string HeartbeatReply::serialize() const {
  return folly::toJson(folly::dynamic::object("registered", registered));
}

HeartbeatReply HeartbeatReply::deserialize(const std::string& bytes) {
  return HeartbeatReply{parse(bytes, "HeartbeatReply")["registered"].asBool()};
}

std::string RemoveShuffle::serialize() const {
  return folly::toJson(folly::dynamic::object("shuffleId", shuffleId));
}

RemoveShuffle RemoveShuffle::deserialize(const std::string& bytes) {
  return RemoveShuffle{
      static_cast<int32_t>(parse(bytes, "RemoveShuffle")["shuffleId"].asInt())};
}

std::string FetchBlock::serialize() const {
  return folly::toJson(folly::dynamic::object("shuffleId", shuffleId)(
      "mapId", mapId)("reduceId", reduceId));
}

FetchBlock FetchBlock::deserialize(const std::string& bytes) {
  auto obj = parse(bytes, "FetchBlock");
  return FetchBlock{
      static_cast<int32_t>(obj["shuffleId"].asInt()),
      static_cast<int32_t>(obj["mapId"].asInt()),
      static_cast<int32_t>(obj["reduceId"].asInt())};
}

} // namespace ember::rpc
