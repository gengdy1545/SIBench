#include <jni.h>
#include <malloc.h>
#include <rocksdb/comparator.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/version.h>
#include <string>

namespace {
void fail(JNIEnv* env, const char* type, const std::string& message) {
    env->ThrowNew(env->FindClass(type), message.c_str());
}
}

extern "C" JNIEXPORT void JNICALL
Java_org_sibench_generation_RocksDbTimestamps_releaseMemory(JNIEnv*, jclass) {
    malloc_trim(0);
}

extern "C" JNIEXPORT void JNICALL
Java_org_sibench_generation_RocksDbTimestamps_requireVersion(JNIEnv* env, jclass, jstring expected) {
    const char* text = env->GetStringUTFChars(expected, nullptr);
    if (!text) return;
    const std::string actual = rocksdb::GetRocksVersionAsString();
    const std::string headers = std::to_string(ROCKSDB_MAJOR) + "." +
        std::to_string(ROCKSDB_MINOR) + "." + std::to_string(ROCKSDB_PATCH);
    const bool matches = actual == text && actual == headers;
    env->ReleaseStringUTFChars(expected, text);
    if (!matches) fail(env, "java/lang/LinkageError", "RocksDB timestamp bridge/runtime/header version mismatch");
}

extern "C" JNIEXPORT void JNICALL
Java_org_sibench_generation_RocksDbTimestamps_comparator(JNIEnv*, jclass, jlong options) {
    reinterpret_cast<rocksdb::ColumnFamilyOptions*>(options)->comparator = rocksdb::BytewiseComparatorWithU64Ts();
}

extern "C" JNIEXPORT void JNICALL
Java_org_sibench_generation_RocksDbTimestamps_commitTimestamp(JNIEnv* env, jclass, jlong transaction, jlong version) {
    const auto status = reinterpret_cast<rocksdb::Transaction*>(transaction)->SetCommitTimestamp(version);
    if (!status.ok()) fail(env, "org/rocksdb/RocksDBException", status.ToString());
}
