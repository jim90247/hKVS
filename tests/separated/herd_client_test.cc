#include "separated/herd_client.h"

#include <catch2/catch.hpp>
#include <map>
#include <memory>

constexpr int kGlobalClientId = 0;
constexpr int kNumServerPorts = 1;
constexpr int kNumClientPorts = 1;
constexpr int kBaseIbPortIndex = 0;

std::unique_ptr<HerdClient> SetupHerdClient() {
  INFO("Expecting " << NUM_WORKERS << " worker threads at server.");
  if (NUM_CLIENTS > 1) {
    WARN("Using more than one client threads");
  }
  auto cli = std::make_unique<HerdClient>(kGlobalClientId, kNumServerPorts,
                                          kNumClientPorts, kBaseIbPortIndex);
  cli->ConnectToServer();
  return cli;
}

TEST_CASE("Random read/write", "[herd_cli]") {
  auto cli = SetupHerdClient();
  std::vector<HerdResp> resps;
  std::map<uint64_t, std::string> local_map;
  constexpr int kTestIters = 1000;

  for (int i = 1; i <= kTestIters; i++) {
    auto mica_k = ConvertPlainKeyToHerdKey(i);
    auto val = fmt::format("value_{:04d}", i);
    while (!cli->PostRequest(ConvertPlainKeyToHerdKey(i), val.c_str(),
                             HERD_VALUE_SIZE, true, i % NUM_WORKERS)) {
      cli->GetResponses(resps);
    }
    local_map[mica_k.second] = val;
  }
  cli->GetResponses(resps);

  auto check_resp = [&](const HerdResp& resp) {
    std::string received;
    for (auto p = resp.ValuePtr(); *p != '\0'; p++) {
      received.push_back(*p);
    }
    const auto& expected = local_map[resp.Key().second];
    REQUIRE(received == expected);
  };

  for (int i = 1; i <= kTestIters; i++) {
    while (!cli->PostRequest(ConvertPlainKeyToHerdKey(i), nullptr, 0, false,
                             i % NUM_WORKERS)) {
      cli->GetResponses(resps);
      for (auto& resp : resps) {
        check_resp(resp);
      }
    }
  }
  cli->GetResponses(resps);
  for (auto& resp : resps) {
    check_resp(resp);
  }
}
