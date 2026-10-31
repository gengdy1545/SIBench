// Exercise the production recorder without making network requests or opening an index.
#include "rocksdb_cloud_jni.cc"
#include <cassert>

int main() {
  Requests requests;
  auto read = static_cast<CloudRequestOpType>(0);
  for (uint64_t micros : {0, 1, 2, 5, 6, 7, 8, 17, 32})
    RecordRequest(&requests, read, 7, micros, micros != 5);
  assert(requests.count[0] == 9 && requests.failed[0] == 1);
  assert(requests.bytes[0] == 63 && requests.micros[0] == 78);
  // Hand-binned durations: [0,1], [2], [], [5,6,7,8], [], [17,32].
  const uint64_t expected[] = {2, 1, 0, 4, 0, 2};
  for (int i = 0; i < kLatencyBuckets; i++)
    assert(requests.latency[0][i] == (i < 6 ? expected[i] : 0));
  Requests overflow;
  RecordRequest(&overflow, read, 0, uint64_t{1} << 40, true);
  assert(overflow.latency[0][31] == 1);
  RecordRequest(&overflow, static_cast<CloudRequestOpType>(-1), 7, 1, false);
  assert(overflow.count[0] == 1);
}
