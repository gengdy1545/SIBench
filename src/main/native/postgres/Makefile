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
	  if ! grep -q 'sib_wrap_receiver' "$(PG_SRC)/src/backend/executor/execMain.c"; then \
	    cd "$(PG_SRC)" && patch --batch --forward -p1 < "$(CURDIR)/postgresql-14.24.patch"; \
	  fi && touch "$(PG_SRC)/.sibench-patched"; \
	fi
	@if ! test -f "$(PG_SRC)/.sibench-execution-completion"; then \
	  if ! grep -q 'sib_emit\("executor_run_end"' "$(PG_SRC)/src/backend/executor/execMain.c"; then \
	    cd "$(PG_SRC)" && patch --batch --forward -p1 < "$(CURDIR)/execution-completion.patch"; \
	  fi && touch "$(PG_SRC)/.sibench-execution-completion"; \
	fi
	@if ! test -f "$(PG_SRC)/.sibench-node-attribution"; then \
	  if ! grep -q 'sib_plan_scan' "$(PG_SRC)/src/backend/executor/nodeIndexscan.c"; then \
	    cd "$(PG_SRC)" && patch --batch --forward -p1 < "$(CURDIR)/node-attribution.patch"; \
	  fi && touch "$(PG_SRC)/.sibench-node-attribution"; \
	fi
	@if ! test -f "$(PG_SRC)/.sibench-access-visibility"; then \
	  if ! grep -q 'sib_bitmap_binding' "$(PG_SRC)/src/backend/executor/nodeBitmapHeapscan.c"; then \
	    cd "$(PG_SRC)" && patch --batch --forward -p1 < "$(CURDIR)/access-visibility.patch"; \
	  fi && touch "$(PG_SRC)/.sibench-access-visibility"; \
	fi
	cp sibench_capture.h sibench_receiver.h "$(PG_SRC)/src/include/"
	touch "$(PG_SRC)/src/backend/access/index/indexam.c" "$(PG_SRC)/src/backend/nodes/tidbitmap.c" \
	  "$(PG_SRC)/src/backend/executor/execMain.c" "$(PG_SRC)/src/backend/catalog/index.c" \
	  "$(PG_SRC)/src/backend/access/transam/xact.c" "$(PG_SRC)/src/backend/executor/nodeIndexscan.c" \
	  "$(PG_SRC)/src/backend/executor/nodeIndexonlyscan.c" "$(PG_SRC)/src/backend/executor/nodeBitmapIndexscan.c" \
	  "$(PG_SRC)/src/backend/executor/nodeBitmapHeapscan.c" "$(PG_SRC)/src/backend/access/heap/heapam_handler.c"

build: patch
	cd "$(PG_SRC)" && ./configure --prefix="$(PREFIX)" --without-readline --without-zlib
	$(MAKE) -C "$(PG_SRC)" -j$(JOBS)

install: build
	$(MAKE) -C "$(PG_SRC)" install

extension:
	mkdir -p "$(dir $(PREFIX))"
	$(CC) -fPIC -shared -I "$(PG_SRC)/src/include" -I . -o "$(dir $(PREFIX))/row_capture.so" row_capture.c
