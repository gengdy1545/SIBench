#include <jni.h>
#include <malloc.h>
#include <rocksdb/comparator.h>
#include <rocksdb/file_checksum.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/version.h>
#include <string>
#define OPENSSL_SUPPRESS_DEPRECATED
#include <openssl/sha.h>
#include <map>
#include <mutex>

namespace {
void fail(JNIEnv* env, const char* type, const std::string& message) {
    env->ThrowNew(env->FindClass(type), message.c_str());
}

struct Digests {
    std::mutex mutex;
    std::map<std::string, std::pair<uint64_t, std::string>> files;
};

class Sha256 final : public rocksdb::FileChecksumGenerator {
    SHA256_CTX state_;
    std::shared_ptr<Digests> digests_;
    std::string name_, checksum_;
    uint64_t bytes_ = 0;
public:
    Sha256(std::shared_ptr<Digests> digests, const std::string& name)
        : digests_(std::move(digests)), name_(name.substr(name.find_last_of('/') + 1)) {
        if (SHA256_Init(&state_) != 1) std::terminate();
    }
    void Update(const char* data, size_t size) override {
        if (SHA256_Update(&state_, data, size) != 1) std::terminate();
        bytes_ += size;
    }
    void Finalize() override {
        try {
            unsigned char digest[SHA256_DIGEST_LENGTH];
            if (SHA256_Final(digest, &state_) != 1) std::terminate();
            static const char hex[] = "0123456789abcdef";
            for (unsigned char byte : digest) { checksum_ += hex[byte >> 4]; checksum_ += hex[byte & 15]; }
            std::lock_guard<std::mutex> lock(digests_->mutex);
            digests_->files.emplace(name_, std::make_pair(bytes_, checksum_));
        } catch (...) { std::terminate(); }
    }
    std::string GetChecksum() const override { return checksum_; }
    const char* Name() const override { return "SIBenchSHA256"; }
};

class Sha256Factory final : public rocksdb::FileChecksumGenFactory {
public:
    std::shared_ptr<Digests> digests = std::make_shared<Digests>();
    std::unique_ptr<rocksdb::FileChecksumGenerator> CreateFileChecksumGenerator(
            const rocksdb::FileChecksumGenContext& context) override {
        try { return std::make_unique<Sha256>(digests, context.file_name); }
        catch (...) { std::terminate(); }
    }
    const char* Name() const override { return "SIBenchSHA256"; }
};
}

extern "C" JNIEXPORT void JNICALL
Java_org_sibench_generation_RocksDbTimestamps_releaseMemory(JNIEnv*, jclass) {
    malloc_trim(0);
}

extern "C" JNIEXPORT void JNICALL
Java_org_sibench_generation_RocksDbTimestamps_fileChecksums(JNIEnv*, jclass, jlong options) {
    reinterpret_cast<rocksdb::DBOptions*>(options)->file_checksum_gen_factory = std::make_shared<Sha256Factory>();
}

extern "C" JNIEXPORT jobject JNICALL
Java_org_sibench_generation_RocksDbTimestamps_writtenFiles(JNIEnv* env, jclass, jlong options) {
    auto base = reinterpret_cast<rocksdb::DBOptions*>(options)->file_checksum_gen_factory;
    if (!base || std::string(base->Name()) != "SIBenchSHA256") {
        fail(env, "java/lang/IllegalStateException", "Missing native file checksum factory"); return nullptr;
    }
    auto factory = std::static_pointer_cast<Sha256Factory>(base);
    jclass map_class = env->FindClass("java/util/HashMap");
    jobject result = env->NewObject(map_class, env->GetMethodID(map_class, "<init>", "()V"));
    jmethodID put = env->GetMethodID(map_class, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    jclass file_class = env->FindClass("org/sibench/generation/RocksDbData$File");
    jmethodID constructor = env->GetMethodID(file_class, "<init>", "(JLjava/lang/String;)V");
    std::lock_guard<std::mutex> lock(factory->digests->mutex);
    for (const auto& entry : factory->digests->files) {
        jstring name = env->NewStringUTF(entry.first.c_str());
        jstring checksum = env->NewStringUTF(entry.second.second.c_str());
        jobject file = env->NewObject(file_class, constructor, static_cast<jlong>(entry.second.first), checksum);
        env->CallObjectMethod(result, put, name, file);
        env->DeleteLocalRef(name); env->DeleteLocalRef(checksum); env->DeleteLocalRef(file);
        if (env->ExceptionCheck()) return nullptr;
    }
    return result;
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
