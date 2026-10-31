# Links the rocksdb-cloud JNI binding from prebuilt static libraries (scripts/build-rocksdb-cloud.sh).
# Only Java_* symbols are exported, so this RocksDB copy never interposes on another in the JVM.
ROOT := $(abspath ../../../../..)
ROCKSDB_CLOUD ?= $(ROOT)/build/index/rocksdb-cloud
AWS_PREFIX ?= $(ROOT)/build/index/aws-sdk
JAVA_HOME ?= $(shell dirname $$(dirname $$(readlink -f $$(command -v javac))))
OUT ?= $(ROOT)/build/index/librocksdb_cloud_jni.so
TEST_OUT ?= $(dir $(OUT))request_latency_test

CXXFLAGS = -std=c++17 -O2 -fPIC -DROCKSDB_PLATFORM_POSIX -DOS_LINUX -DUSE_AWS \
	-I$(ROCKSDB_CLOUD)/include -I$(AWS_PREFIX)/include \
	-I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux
LIBS = $(ROCKSDB_CLOUD)/librocksdb.a \
	-L$(AWS_PREFIX)/lib64 -L$(AWS_PREFIX)/lib \
	-laws-cpp-sdk-transfer -laws-cpp-sdk-s3 -laws-cpp-sdk-kinesis -laws-cpp-sdk-core \
	-laws-c-event-stream -laws-checksums -laws-c-common \
	-lcurl -lssl -lcrypto -lsnappy -lz -lbz2 -llz4 -lzstd -lpthread -lrt -ldl

$(OUT): rocksdb_cloud_jni.cc jni_exports.map
	mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -shared -o $@ rocksdb_cloud_jni.cc $(LIBS) \
		-Wl,--version-script=jni_exports.map -Wl,--no-undefined

clean:
	rm -f $(OUT) $(TEST_OUT)

$(TEST_OUT): ../../../../test/native/request_latency_test.cc rocksdb_cloud_jni.cc
	mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -I. -o $@ $< $(LIBS)

test: $(TEST_OUT)
	$(TEST_OUT)

.PHONY: clean test
