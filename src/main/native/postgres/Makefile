# Build only a separately extracted PostgreSQL 14.24 source tree.
ROOT := $(abspath ../../../..)
PG_SRC ?= $(ROOT)/build/benchmarks/postgresql-14.24
PREFIX ?= $(ROOT)/build/services/postgresql-capture/runtime
JOBS ?= 8

.PHONY: patch build install extension
patch:
	@test -f "$(PG_SRC)/configure" || (echo "Extract PostgreSQL 14.24 into PG_SRC first"; exit 1)
	@grep -q 'PACKAGE_VERSION='\''14.24'\''' "$(PG_SRC)/configure"
	@if ! test -f "$(PG_SRC)/.sibench-patched"; then \
	  cd "$(PG_SRC)" && patch --batch --forward -p1 < "$(CURDIR)/postgresql-14.24.patch" && touch .sibench-patched; \
	fi
	cp sibench_capture.h sibench_receiver.h "$(PG_SRC)/src/include/"
	touch "$(PG_SRC)/src/backend/access/index/indexam.c" "$(PG_SRC)/src/backend/nodes/tidbitmap.c" \
	  "$(PG_SRC)/src/backend/executor/execMain.c" "$(PG_SRC)/src/backend/catalog/index.c" \
	  "$(PG_SRC)/src/backend/access/transam/xact.c"

build: patch
	cd "$(PG_SRC)" && ./configure --prefix="$(PREFIX)" --without-readline --without-zlib
	$(MAKE) -C "$(PG_SRC)" -j$(JOBS)

install: build
	$(MAKE) -C "$(PG_SRC)" install

extension:
	mkdir -p "$(dir $(PREFIX))"
	$(CC) -fPIC -shared -I "$(PG_SRC)/src/include" -I . -o "$(dir $(PREFIX))/row_capture.so" row_capture.c
