// JNI binding of rocksdb-cloud as a plain ordered byte store for
// org.sibench.adapters.CloudRocksStore. It forwards keys and values only: the index layout,
// versions and visibility stay in Java (RocksDbBackend).
#include <jni.h>

#include <array>
#include <atomic>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>

#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/version.h"

using namespace ROCKSDB_NAMESPACE;

namespace {

// Object-storage requests by CloudRequestOpType, from rocksdb-cloud's request callback.
constexpr int kRequestTypes = 7;
constexpr int kLatencyBuckets = 32;
const char* const kRequestNames[kRequestTypes] = {"read", "write", "list", "create",
                                                  "delete", "copy", "info"};

struct Requests {
  std::array<std::atomic<uint64_t>, kRequestTypes> count{}, failed{}, bytes{}, micros{};
  std::array<std::array<std::atomic<uint64_t>, kLatencyBuckets>, kRequestTypes> latency{};
};

void RecordRequest(Requests* requests, CloudRequestOpType type, uint64_t size,
                   uint64_t micros, bool ok) {
  int i = static_cast<int>(type);
  if (i < 0 || i >= kRequestTypes) return;
  requests->count[i]++;
  if (!ok) requests->failed[i]++;
  requests->bytes[i] += size;
  requests->micros[i] += micros;
  // Upper bounds are 1, 2, 4, ... microseconds; the last bucket includes overflow.
  unsigned bucket = micros <= 1 ? 0 : 64 - __builtin_clzll(micros - 1);
  if (bucket >= kLatencyBuckets) bucket = kLatencyBuckets - 1;
  requests->latency[i][bucket].fetch_add(1, std::memory_order_relaxed);
}

struct Store {
  Requests requests;
  std::shared_ptr<FileSystem> fs;
  std::unique_ptr<Env> env;
  DBCloud* db = nullptr;
  WriteOptions logged;
  WriteOptions unlogged;
};

struct Cursor {
  std::string lower, upper;
  Slice lower_slice, upper_slice;
  ReadOptions read;
  std::unique_ptr<Iterator> it;
};

void InitAws() {
  static std::once_flag once;
  // The SDK stays initialised for the process lifetime: a store may be reopened after close.
  std::call_once(once, [] {
    static Aws::SDKOptions options;
    Aws::InitAPI(options);
  });
}

void ThrowIo(JNIEnv* env, const std::string& message) {
  env->ThrowNew(env->FindClass("java/io/IOException"), message.c_str());
}

std::string Text(JNIEnv* env, jstring value) {
  if (value == nullptr) return {};
  const char* chars = env->GetStringUTFChars(value, nullptr);
  std::string text(chars);
  env->ReleaseStringUTFChars(value, chars);
  return text;
}

std::string Bytes(JNIEnv* env, jbyteArray value) {
  jsize size = env->GetArrayLength(value);
  std::string bytes(static_cast<size_t>(size), '\0');
  env->GetByteArrayRegion(value, 0, size, reinterpret_cast<jbyte*>(&bytes[0]));
  return bytes;
}

jbyteArray Array(JNIEnv* env, const Slice& value) {
  jbyteArray array = env->NewByteArray(static_cast<jsize>(value.size()));
  env->SetByteArrayRegion(array, 0, static_cast<jsize>(value.size()),
                          reinterpret_cast<const jbyte*>(value.data()));
  return array;
}

Store* AsStore(jlong handle) { return reinterpret_cast<Store*>(handle); }
Cursor* AsCursor(jlong handle) { return reinterpret_cast<Cursor*>(handle); }

}  // namespace

extern "C" {

JNIEXPORT jstring JNICALL Java_org_sibench_adapters_CloudRocksStore_version(JNIEnv* env, jclass) {
  std::string version = "rocksdb-cloud-" + std::to_string(ROCKSDB_MAJOR) + "." +
                        std::to_string(ROCKSDB_MINOR) + "." + std::to_string(ROCKSDB_PATCH);
  return env->NewStringUTF(version.c_str());
}

JNIEXPORT jlong JNICALL Java_org_sibench_adapters_CloudRocksStore_open(
    JNIEnv* env, jclass, jstring path, jstring bucket, jstring object_path, jstring region,
    jstring endpoint, jboolean virtual_addressing, jstring access_key, jstring secret_key,
    jstring session_token, jboolean create) {
  InitAws();
  auto store = std::make_unique<Store>();
  Requests* requests = &store->requests;
  CloudFileSystemOptions cloud;
  cloud.cloud_request_callback = std::make_shared<CloudRequestCallback>(
      [requests](CloudRequestOpType type, uint64_t size, uint64_t micros, bool ok) {
        RecordRequest(requests, type, size, micros, ok);
      });
  cloud.credentials.provider = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
      Text(env, access_key).c_str(), Text(env, secret_key).c_str(),
      Text(env, session_token).c_str());
  for (BucketOptions* options : {&cloud.src_bucket, &cloud.dest_bucket}) {
    options->SetBucketPrefix("");
    options->SetBucketName(Text(env, bucket));
    options->SetObjectPath(Text(env, object_path));
    options->SetRegion(Text(env, region));
  }
  cloud.keep_local_sst_files = true;
  cloud.keep_local_log_files = true;
  cloud.create_bucket_if_missing = false;
  std::string host = Text(env, endpoint);
  if (!host.empty()) {
    auto scheme = Aws::Http::Scheme::HTTPS;
    if (host.rfind("http://", 0) == 0) {
      scheme = Aws::Http::Scheme::HTTP;
      host = host.substr(7);
    } else if (host.rfind("https://", 0) == 0) {
      host = host.substr(8);
    }
    bool virtual_hosted = virtual_addressing;
    cloud.s3_client_factory =
        [host, scheme, virtual_hosted](
            const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>& credentials,
            const Aws::Client::ClientConfiguration& base) {
          Aws::Client::ClientConfiguration config(base);
          config.endpointOverride = host;
          config.scheme = scheme;
          return std::make_shared<Aws::S3::S3Client>(
              credentials, config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
              virtual_hosted);
        };
  }
  CloudFileSystem* cfs = nullptr;
  Status status =
      CloudFileSystemEnv::NewAwsFileSystem(FileSystem::Default(), cloud, nullptr, &cfs);
  if (!status.ok()) {
    ThrowIo(env, "rocksdb-cloud file system: " + status.ToString());
    return 0;
  }
  store->fs.reset(cfs);
  store->env = CloudFileSystemEnv::NewCompositeEnv(Env::Default(), store->fs);
  Options options;
  options.env = store->env.get();
  // Reopening may start from the bucket alone; the caller refuses a database it did not create.
  options.create_if_missing = true;
  options.error_if_exists = create;
  status = DBCloud::Open(options, Text(env, path), "", 0, &store->db);
  if (!status.ok()) {
    ThrowIo(env, "rocksdb-cloud open: " + status.ToString());
    return 0;
  }
  store->unlogged.disableWAL = true;
  return reinterpret_cast<jlong>(store.release());
}

JNIEXPORT void JNICALL Java_org_sibench_adapters_CloudRocksStore_put(
    JNIEnv* env, jclass, jlong handle, jbyteArray key, jbyteArray value, jboolean wal) {
  Store* store = AsStore(handle);
  Status status =
      store->db->Put(wal ? store->logged : store->unlogged, Bytes(env, key), Bytes(env, value));
  if (!status.ok()) ThrowIo(env, status.ToString());
}

JNIEXPORT jbyteArray JNICALL Java_org_sibench_adapters_CloudRocksStore_get(
    JNIEnv* env, jclass, jlong handle, jbyteArray key) {
  std::string value;
  Status status = AsStore(handle)->db->Get(ReadOptions(), Bytes(env, key), &value);
  if (status.IsNotFound()) return nullptr;
  if (!status.ok()) {
    ThrowIo(env, status.ToString());
    return nullptr;
  }
  return Array(env, value);
}

JNIEXPORT jlong JNICALL Java_org_sibench_adapters_CloudRocksStore_cursor(
    JNIEnv* env, jclass, jlong handle, jbyteArray lower, jbyteArray upper) {
  auto cursor = std::make_unique<Cursor>();
  if (lower != nullptr) {
    cursor->lower = Bytes(env, lower);
    cursor->lower_slice = Slice(cursor->lower);
    cursor->read.iterate_lower_bound = &cursor->lower_slice;
  }
  if (upper != nullptr) {
    cursor->upper = Bytes(env, upper);
    cursor->upper_slice = Slice(cursor->upper);
    cursor->read.iterate_upper_bound = &cursor->upper_slice;
  }
  cursor->it.reset(AsStore(handle)->db->NewIterator(cursor->read));
  return reinterpret_cast<jlong>(cursor.release());
}

JNIEXPORT void JNICALL Java_org_sibench_adapters_CloudRocksStore_seek(
    JNIEnv* env, jclass, jlong handle, jbyteArray key) {
  AsCursor(handle)->it->Seek(Bytes(env, key));
}

JNIEXPORT void JNICALL Java_org_sibench_adapters_CloudRocksStore_seekForPrev(
    JNIEnv* env, jclass, jlong handle, jbyteArray key) {
  AsCursor(handle)->it->SeekForPrev(Bytes(env, key));
}

JNIEXPORT void JNICALL Java_org_sibench_adapters_CloudRocksStore_seekToLast(JNIEnv*, jclass,
                                                                            jlong handle) {
  AsCursor(handle)->it->SeekToLast();
}

JNIEXPORT void JNICALL Java_org_sibench_adapters_CloudRocksStore_next(JNIEnv*, jclass,
                                                                      jlong handle) {
  AsCursor(handle)->it->Next();
}

JNIEXPORT void JNICALL Java_org_sibench_adapters_CloudRocksStore_prev(JNIEnv*, jclass,
                                                                      jlong handle) {
  AsCursor(handle)->it->Prev();
}

JNIEXPORT jboolean JNICALL Java_org_sibench_adapters_CloudRocksStore_valid(JNIEnv* env, jclass,
                                                                           jlong handle) {
  Iterator* it = AsCursor(handle)->it.get();
  if (it->Valid()) return JNI_TRUE;
  if (!it->status().ok()) ThrowIo(env, it->status().ToString());
  return JNI_FALSE;
}

JNIEXPORT jbyteArray JNICALL Java_org_sibench_adapters_CloudRocksStore_key(JNIEnv* env, jclass,
                                                                           jlong handle) {
  return Array(env, AsCursor(handle)->it->key());
}

JNIEXPORT jbyteArray JNICALL Java_org_sibench_adapters_CloudRocksStore_value(JNIEnv* env, jclass,
                                                                             jlong handle) {
  return Array(env, AsCursor(handle)->it->value());
}

JNIEXPORT void JNICALL Java_org_sibench_adapters_CloudRocksStore_closeCursor(JNIEnv*, jclass,
                                                                             jlong handle) {
  delete AsCursor(handle);
}

JNIEXPORT jstring JNICALL Java_org_sibench_adapters_CloudRocksStore_property(JNIEnv* env, jclass,
                                                                             jlong handle,
                                                                             jstring name) {
  std::string value;
  if (!AsStore(handle)->db->GetProperty(Text(env, name), &value)) return nullptr;
  return env->NewStringUTF(value.c_str());
}

// One line per type: name, requests, failed, bytes, microseconds, 32 latency buckets.
JNIEXPORT jstring JNICALL Java_org_sibench_adapters_CloudRocksStore_requests(JNIEnv* env, jclass,
                                                                             jlong handle) {
  const Requests& requests = AsStore(handle)->requests;
  std::ostringstream out;
  for (int i = 0; i < kRequestTypes; i++) {
    out << kRequestNames[i] << ' ' << requests.count[i] << ' ' << requests.failed[i] << ' '
        << requests.bytes[i] << ' ' << requests.micros[i];
    for (const auto& bucket : requests.latency[i]) out << ' ' << bucket.load(std::memory_order_relaxed);
    out << '\n';
  }
  return env->NewStringUTF(out.str().c_str());
}

// Returns once the memtables are SST files, which rocksdb-cloud uploads when they close, and the
// MANIFEST, which it uploads on every sync, is in the bucket; the WAL stays local.
JNIEXPORT void JNICALL Java_org_sibench_adapters_CloudRocksStore_flush(JNIEnv* env, jclass,
                                                                       jlong handle) {
  Store* store = AsStore(handle);
  FlushOptions flush;
  flush.wait = true;
  Status status = store->db->Flush(flush);
  if (status.ok()) status = store->db->FlushWAL(true);
  if (!status.ok()) ThrowIo(env, status.ToString());
}

JNIEXPORT void JNICALL Java_org_sibench_adapters_CloudRocksStore_close(JNIEnv* env, jclass,
                                                                       jlong handle) {
  Store* store = AsStore(handle);
  Status status = store->db->Close();
  delete store->db;
  store->env.reset();
  store->fs.reset();
  delete store;
  if (!status.ok()) ThrowIo(env, status.ToString());
}

}  // extern "C"
