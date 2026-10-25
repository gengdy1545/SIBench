# Build only a separately extracted PostgreSQL 14.24 source tree.
ROOT := $(abspath ../../../..)
PG_SRC ?= $(ROOT)/build/benchmarks/postgresql-14.24
PREFIX ?= $(ROOT)/build/services/postgresql-capture/runtime
JOBS ?= 8

.PHONY: patch build install extension
# One patch against pristine PostgreSQL 14.24 hooks the backend into sibench_capture.c, which is
# copied into the tree with its header. The marker keeps a patched tree from being patched again; a
# tree with an earlier patch must be extracted afresh. The backend links libzstd (libzstd-dev).
patch:
	@test -f "$(PG_SRC)/configure" || (echo "Extract PostgreSQL 14.24 into PG_SRC first"; exit 1)
	@grep -q 'PACKAGE_VERSION='\''14.24'\''' "$(PG_SRC)/configure"
	@if ! test -f "$(PG_SRC)/.sibench-patched-3"; then \
	  test ! -f "$(PG_SRC)/.sibench-patched" -a ! -f "$(PG_SRC)/.sibench-patched-2" || \
	    (echo "PG_SRC carries an older SIBench patch; extract a fresh PostgreSQL 14.24"; exit 1); \
	  cd "$(PG_SRC)" && patch --batch --forward -p1 < "$(CURDIR)/postgresql-14.24.patch" && \
	  touch "$(PG_SRC)/.sibench-patched-3"; \
	fi
	cp sibench_capture.h "$(PG_SRC)/src/include/"
	cp sibench_capture.c "$(PG_SRC)/src/backend/access/index/"
	# Every file including the copied header rebuilds; the patched files are the ones that do.
	cd "$(PG_SRC)" && grep -l 'sibench_capture.h' $$(grep '^+++ b/' "$(CURDIR)/postgresql-14.24.patch" | sed 's|^+++ b/||') \
	  src/backend/access/index/sibench_capture.c | xargs touch

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
