# Build only a separately extracted PostgreSQL 14.24 source tree.
ROOT := $(abspath ../../../..)
PG_SRC ?= $(ROOT)/build/benchmarks/postgresql-14.24
PREFIX ?= $(ROOT)/build/services/postgresql-capture/runtime
JOBS ?= 8

.PHONY: patch build install extension
# One patch against pristine PostgreSQL 14.24; the marker keeps a patched tree from being patched
# again, and the sentinel recognises a tree patched without it.
patch:
	@test -f "$(PG_SRC)/configure" || (echo "Extract PostgreSQL 14.24 into PG_SRC first"; exit 1)
	@grep -q 'PACKAGE_VERSION='\''14.24'\''' "$(PG_SRC)/configure"
	@if ! test -f "$(PG_SRC)/.sibench-patched"; then \
	  if ! grep -q 'sib_heap_visibility' "$(PG_SRC)/src/backend/access/index/indexam.c"; then \
	    cd "$(PG_SRC)" && patch --batch --forward -p1 < "$(CURDIR)/postgresql-14.24.patch"; \
	  fi && touch "$(PG_SRC)/.sibench-patched"; \
	fi
	cp sibench_capture.h sibench_receiver.h "$(PG_SRC)/src/include/"
	# The instrumentation lives in the copied headers, so every file including them rebuilds.
	touch "$(PG_SRC)/src/backend/access/index/indexam.c" "$(PG_SRC)/src/backend/nodes/tidbitmap.c" \
	  "$(PG_SRC)/src/backend/executor/execMain.c" "$(PG_SRC)/src/backend/catalog/index.c" \
	  "$(PG_SRC)/src/backend/access/transam/xact.c" "$(PG_SRC)/src/backend/executor/nodeIndexscan.c" \
	  "$(PG_SRC)/src/backend/executor/nodeIndexonlyscan.c" "$(PG_SRC)/src/backend/executor/nodeBitmapIndexscan.c" \
	  "$(PG_SRC)/src/backend/executor/nodeBitmapHeapscan.c" "$(PG_SRC)/src/backend/access/heap/heapam_handler.c" \
	  "$(PG_SRC)/src/backend/storage/ipc/procarray.c"

# The patch adds a field to PGPROC (storage/proc.h) and PostgreSQL builds without dependency
# tracking, so an in-place rebuild after a header change needs `make -C $(PG_SRC) clean` first.
build: patch
	cd "$(PG_SRC)" && ./configure --prefix="$(PREFIX)" --without-readline --without-zlib
	$(MAKE) -C "$(PG_SRC)" -j$(JOBS)

install: build
	$(MAKE) -C "$(PG_SRC)" install

extension:
	mkdir -p "$(dir $(PREFIX))"
	$(CC) -fPIC -shared -I "$(PG_SRC)/src/include" -I . -o "$(dir $(PREFIX))/row_capture.so" row_capture.c
