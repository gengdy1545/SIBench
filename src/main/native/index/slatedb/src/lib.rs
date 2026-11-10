//! JNI byte storage only. Java owns index keys, versions, Trace semantics and result validation.
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use jni::{
    objects::{JByteArray, JClass, JObjectArray, JString},
    sys::{jboolean, jbyteArray, jint, jlong, jstring},
    JNIEnv,
};
use slatedb::{
    config::{ScanOptions, Settings},
    object_store::{
        aws::AmazonS3Builder, path::Path, CopyOptions, GetOptions, GetResult, ListResult,
        MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions,
        PutOptions, PutPayload, PutResult, UploadPart,
    },
    Db, DbIterator, DbSnapshot, IterationOrder, SlateDbMirrorPolicy, WriteBatch,
};
use slatedb_mirror::ObjectStoreMirror;
use std::{
    collections::HashMap,
    ops::Bound,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc, Mutex, OnceLock, RwLock,
    },
    time::Instant,
};
use tokio::runtime::Runtime;

type Result<T> = std::result::Result<T, String>;
static NEXT: AtomicI64 = AtomicI64::new(1);
static STORES: OnceLock<RwLock<HashMap<i64, Arc<Store>>>> = OnceLock::new();
/// Every index lookup opens, moves and closes a cursor, so one registry lock would serialize them.
const CURSOR_SHARDS: usize = 64;
static CURSORS: OnceLock<[Mutex<HashMap<i64, Arc<Mutex<Cursor>>>>; CURSOR_SHARDS]> =
    OnceLock::new();
const OWNER: &[u8] = b"SIBench SlateDB byte store\n";

struct Store {
    runtime: Runtime,
    db: Db,
    requests: Arc<Requests>,
}

/// Remote request statistics in the rocksdb-cloud layout: per type, the count, failures, bytes,
/// microseconds and 32 latency buckets with power-of-two microsecond upper bounds.
const REQUEST_TYPES: [&str; 7] = ["read", "write", "list", "create", "delete", "copy", "info"];
const READ: usize = 0;
const WRITE: usize = 1;
const LIST: usize = 2;
const DELETE: usize = 4;
const COPY: usize = 5;
const INFO: usize = 6;

struct Requests {
    counters: [[AtomicU64; 36]; 7],
}

impl Default for Requests {
    fn default() -> Self {
        Self {
            counters: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
        }
    }
}

impl std::fmt::Debug for Requests {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Requests")
    }
}

impl Requests {
    fn record(&self, kind: usize, bytes: u64, started: Instant, ok: bool) {
        let micros = started.elapsed().as_micros() as u64;
        let row = &self.counters[kind];
        row[0].fetch_add(1, Ordering::Relaxed);
        row[1].fetch_add(u64::from(!ok), Ordering::Relaxed);
        row[2].fetch_add(bytes, Ordering::Relaxed);
        row[3].fetch_add(micros, Ordering::Relaxed);
        let bucket = if micros <= 1 {
            0
        } else {
            64 - (micros - 1).leading_zeros() as usize
        };
        row[4 + bucket.min(31)].fetch_add(1, Ordering::Relaxed);
    }

    fn text(&self) -> String {
        let mut out = String::new();
        for (name, row) in REQUEST_TYPES.iter().zip(&self.counters) {
            out.push_str(name);
            for value in row {
                out.push(' ');
                out.push_str(&value.load(Ordering::Relaxed).to_string());
            }
            out.push('\n');
        }
        out
    }
}

/// Counts every request that reaches the remote store, below SlateDB and its local mirror.
#[derive(Debug)]
struct Counted {
    inner: Arc<dyn ObjectStore>,
    requests: Arc<Requests>,
}

impl std::fmt::Display for Counted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Counted({})", self.inner)
    }
}

impl Counted {
    fn track<T>(
        &self,
        kind: usize,
        bytes: impl Fn(&T) -> u64,
        started: Instant,
        result: slatedb::object_store::Result<T>,
    ) -> slatedb::object_store::Result<T> {
        self.requests.record(
            kind,
            result.as_ref().map_or(0, &bytes),
            started,
            result.is_ok(),
        );
        result
    }
}

#[derive(Debug)]
struct CountedUpload {
    inner: Box<dyn MultipartUpload>,
    requests: Arc<Requests>,
}

#[async_trait]
impl MultipartUpload for CountedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let (requests, bytes, part) = (
            self.requests.clone(),
            data.content_length() as u64,
            self.inner.put_part(data),
        );
        let started = Instant::now();
        Box::pin(async move {
            let result = part.await;
            requests.record(
                WRITE,
                if result.is_ok() { bytes } else { 0 },
                started,
                result.is_ok(),
            );
            result
        })
    }
    async fn complete(&mut self) -> slatedb::object_store::Result<PutResult> {
        let started = Instant::now();
        let result = self.inner.complete().await;
        self.requests.record(WRITE, 0, started, result.is_ok());
        result
    }
    async fn abort(&mut self) -> slatedb::object_store::Result<()> {
        let started = Instant::now();
        let result = self.inner.abort().await;
        self.requests.record(DELETE, 0, started, result.is_ok());
        result
    }
}

#[async_trait]
impl ObjectStore for Counted {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> slatedb::object_store::Result<PutResult> {
        let (started, bytes) = (Instant::now(), payload.content_length() as u64);
        let result = self.inner.put_opts(location, payload, opts).await;
        self.track(WRITE, |_| bytes, started, result)
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> slatedb::object_store::Result<Box<dyn MultipartUpload>> {
        let started = Instant::now();
        let result = self.inner.put_multipart_opts(location, opts).await;
        let upload = self.track(WRITE, |_| 0, started, result)?;
        Ok(Box::new(CountedUpload {
            inner: upload,
            requests: self.requests.clone(),
        }))
    }
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> slatedb::object_store::Result<GetResult> {
        let (started, head) = (Instant::now(), options.head);
        let result = self.inner.get_opts(location, options).await;
        self.track(
            if head { INFO } else { READ },
            |r: &GetResult| if head { 0 } else { r.range.end - r.range.start },
            started,
            result,
        )
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, slatedb::object_store::Result<Path>>,
    ) -> BoxStream<'static, slatedb::object_store::Result<Path>> {
        let requests = self.requests.clone();
        self.inner
            .delete_stream(locations)
            .map(move |result| {
                requests.record(DELETE, 0, Instant::now(), result.is_ok());
                result
            })
            .boxed()
    }
    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
        // A paged listing is counted once when it starts; its pages are not timed.
        self.requests.record(LIST, 0, Instant::now(), true);
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> slatedb::object_store::Result<ListResult> {
        let started = Instant::now();
        let result = self.inner.list_with_delimiter(prefix).await;
        self.track(LIST, |_| 0, started, result)
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> slatedb::object_store::Result<()> {
        let started = Instant::now();
        let result = self.inner.copy_opts(from, to, options).await;
        self.track(COPY, |_| 0, started, result)
    }
}

struct Cursor {
    store: Arc<Store>,
    snapshot: Arc<DbSnapshot>,
    lower: Option<Bytes>,
    upper: Option<Bytes>,
    iterator: Option<DbIterator>,
    current: Option<(Bytes, Bytes)>,
    reverse: bool,
}

fn stores() -> &'static RwLock<HashMap<i64, Arc<Store>>> {
    STORES.get_or_init(Default::default)
}
fn cursors(id: i64) -> &'static Mutex<HashMap<i64, Arc<Mutex<Cursor>>>> {
    &CURSORS.get_or_init(|| std::array::from_fn(|_| Mutex::default()))[id as usize % CURSOR_SHARDS]
}
fn lock<T>(value: &Mutex<T>) -> Result<std::sync::MutexGuard<'_, T>> {
    value
        .lock()
        .map_err(|_| "Native state lock poisoned".into())
}
fn store(id: i64) -> Result<Arc<Store>> {
    stores()
        .read()
        .map_err(|_| "Native state lock poisoned")?
        .get(&id)
        .cloned()
        .ok_or("SlateDB store is closed".into())
}
fn cursor(id: i64) -> Result<Arc<Mutex<Cursor>>> {
    lock(cursors(id))?
        .get(&id)
        .cloned()
        .ok_or("SlateDB cursor is closed".into())
}
fn error(e: impl std::fmt::Display) -> String {
    e.to_string()
}
fn bytes(env: &JNIEnv, value: &JByteArray) -> Result<Vec<u8>> {
    env.convert_byte_array(value).map_err(error)
}
fn optional_bytes(env: &JNIEnv, value: &JByteArray) -> Result<Option<Bytes>> {
    if value.is_null() {
        Ok(None)
    } else {
        Ok(Some(bytes(env, value)?.into()))
    }
}
fn string(env: &mut JNIEnv, value: &JString) -> Result<String> {
    env.get_string(value).map(|v| v.into()).map_err(error)
}

/// object_store expects a virtual-hosted endpoint to name the bucket already, as COS requires.
fn s3_endpoint(endpoint: &str, bucket: &str, virtual_hosted: bool) -> String {
    match endpoint.split_once("://") {
        Some((scheme, host)) if virtual_hosted && !host.starts_with(&format!("{bucket}.")) => {
            format!("{scheme}://{bucket}.{host}")
        }
        _ => endpoint.to_string(),
    }
}

// Never unwind Rust through the JVM. Registry handles also prevent use-after-free on close races.
fn call<T: Default>(mut env: JNIEnv, body: impl FnOnce(&mut JNIEnv) -> Result<T>) -> T {
    match catch_unwind(AssertUnwindSafe(|| body(&mut env))) {
        Ok(Ok(value)) => value,
        outcome => {
            let message = match outcome {
                Ok(Err(message)) => message,
                _ => "SlateDB native panic".into(),
            };
            let _ = env.throw_new("java/io/IOException", message);
            T::default()
        }
    }
}

impl Store {
    /// `mirror` keeps the compacted SSTs on local disk, as rocksdb-cloud keeps its SST files.
    fn open(
        objects: Arc<dyn ObjectStore>,
        prefix: &str,
        mirror: Option<&std::path::Path>,
        create: bool,
    ) -> Result<Self> {
        let requests = Arc::new(Requests::default());
        let objects: Arc<dyn ObjectStore> = Arc::new(Counted {
            inner: objects,
            requests: requests.clone(),
        });
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .map_err(error)?;
        let db = runtime.block_on(async {
            let prefix = Path::from(prefix);
            let owner = prefix.clone().join("sibench-owner");
            if create {
                if objects
                    .list(Some(&prefix))
                    .try_next()
                    .await
                    .map_err(error)?
                    .is_some()
                {
                    return Err("SlateDB object prefix already exists".into());
                }
                objects
                    .put_opts(
                        &owner,
                        Bytes::from_static(OWNER).into(),
                        PutOptions {
                            mode: PutMode::Create,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(error)?;
            } else {
                let existing = objects
                    .get(&owner)
                    .await
                    .map_err(error)?
                    .bytes()
                    .await
                    .map_err(error)?;
                if existing.as_ref() != OWNER {
                    return Err("Invalid SlateDB object-prefix owner".into());
                }
            }
            let main: Arc<dyn ObjectStore> = match mirror {
                Some(mirror) => ObjectStoreMirror::builder(
                    mirror,
                    objects,
                    SlateDbMirrorPolicy::builder().build(),
                )
                .build()
                .await
                .map_err(error)?,
                None => objects,
            };
            Db::builder(prefix, main)
                .with_settings(Settings {
                    object_store_max_retries: Some(3),
                    ..Settings::default()
                })
                .build()
                .await
                .map_err(error)
        })?;
        Ok(Self {
            runtime,
            db,
            requests,
        })
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.runtime
            .block_on(self.db.put(key, value))
            .map(|_| ())
            .map_err(error)
    }
    fn put_all(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        let mut batch = WriteBatch::new();
        for (key, value) in entries {
            batch.put(key, value);
        }
        self.runtime
            .block_on(self.db.write(batch))
            .map(|_| ())
            .map_err(error)
    }
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.runtime.block_on(self.db.get(key)).map_err(error)
    }
    fn flush(&self) -> Result<()> {
        self.runtime.block_on(self.db.flush()).map_err(error)
    }
    fn close(&self) -> Result<()> {
        self.runtime.block_on(self.db.close()).map_err(error)
    }
}

impl Cursor {
    fn new(store: Arc<Store>, lower: Option<Bytes>, upper: Option<Bytes>) -> Result<Self> {
        let snapshot = store.db.snapshot().map_err(error)?;
        Ok(Self {
            store,
            snapshot,
            lower,
            upper,
            iterator: None,
            current: None,
            reverse: false,
        })
    }

    fn advance(&mut self) -> Result<()> {
        self.current = match self.iterator.as_mut() {
            Some(iter) => self
                .store
                .runtime
                .block_on(iter.next())
                .map_err(error)?
                .map(|kv| (kv.key, kv.value)),
            None => None,
        };
        Ok(())
    }

    fn position(&mut self, target: Option<Bytes>, reverse: bool, inclusive: bool) -> Result<()> {
        self.current = None;
        self.iterator = None;
        self.reverse = reverse;
        let mut lower = self
            .lower
            .clone()
            .map(Bound::Included)
            .unwrap_or(Bound::Unbounded);
        let mut upper = self
            .upper
            .clone()
            .map(Bound::Excluded)
            .unwrap_or(Bound::Unbounded);
        if let Some(target) = target {
            if reverse {
                if self
                    .lower
                    .as_ref()
                    .is_some_and(|v| target < *v || (!inclusive && target == *v))
                {
                    return Ok(());
                }
                if self.upper.as_ref().is_none_or(|v| target < *v) {
                    upper = if inclusive {
                        Bound::Included(target)
                    } else {
                        Bound::Excluded(target)
                    };
                }
            } else {
                if self.upper.as_ref().is_some_and(|v| target >= *v) {
                    return Ok(());
                }
                if self.lower.as_ref().is_none_or(|v| target >= *v) {
                    lower = if inclusive {
                        Bound::Included(target)
                    } else {
                        Bound::Excluded(target)
                    };
                }
            }
        }
        if self
            .lower
            .as_ref()
            .zip(self.upper.as_ref())
            .is_some_and(|(lo, hi)| lo >= hi)
        {
            return Ok(());
        }
        // Index lookups are short seeks that revisit hot blocks, unlike SlateDB's long-scan default.
        let options = ScanOptions::default()
            .with_cache_blocks(true)
            .with_order(if reverse {
                IterationOrder::Descending
            } else {
                IterationOrder::Ascending
            });
        self.iterator = Some(
            self.store
                .runtime
                .block_on(self.snapshot.scan_with_options((lower, upper), &options))
                .map_err(error)?,
        );
        self.advance()
    }

    fn seek(&mut self, target: Bytes) -> Result<()> {
        if !self.reverse
            && self.current.as_ref().is_some_and(|(key, _)| *key < target)
            && self.upper.as_ref().is_none_or(|upper| target < *upper)
        {
            let iter = self.iterator.as_mut().ok_or("Missing SlateDB iterator")?;
            self.store
                .runtime
                .block_on(iter.seek(&target))
                .map_err(error)?;
            return self.advance();
        }
        self.position(Some(target), false, true)
    }

    fn step(&mut self, reverse: bool) -> Result<()> {
        let Some((key, _)) = self.current.clone() else {
            return Ok(());
        };
        if self.reverse == reverse {
            self.advance()
        } else {
            self.position(Some(key), reverse, false)
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_sibench_adapters_SlateDbStore_nativeVersion(
    env: JNIEnv,
    _: JClass,
) -> jstring {
    call(env, |env| {
        env.new_string(concat!(
            "slatedb-",
            env!("SIBENCH_SLATE_VERSION"),
            "@",
            env!("SIBENCH_SLATE_SOURCE")
        ))
        .map(|s| s.into_raw())
        .map_err(error)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_sibench_adapters_SlateDbStore_nativeOpen(
    env: JNIEnv,
    _: JClass,
    bucket: JString,
    prefix: JString,
    region: JString,
    endpoint: JString,
    virtual_hosted: jboolean,
    access: JString,
    secret: JString,
    token: JString,
    mirror: JString,
    create: jboolean,
) -> jlong {
    call(env, |env| {
        let bucket = string(env, &bucket)?;
        let endpoint = s3_endpoint(&string(env, &endpoint)?, &bucket, virtual_hosted != 0);
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(string(env, &region)?)
            .with_endpoint(&endpoint)
            .with_allow_http(endpoint.starts_with("http://"))
            .with_virtual_hosted_style_request(virtual_hosted != 0)
            .with_access_key_id(string(env, &access)?)
            .with_secret_access_key(string(env, &secret)?);
        let token = string(env, &token)?;
        if !token.is_empty() {
            builder = builder.with_token(token);
        }
        let db = Store::open(
            Arc::new(builder.build().map_err(error)?),
            &string(env, &prefix)?,
            Some(std::path::Path::new(&string(env, &mirror)?)),
            create != 0,
        )?;
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        stores()
            .write()
            .map_err(|_| "Native state lock poisoned")?
            .insert(id, Arc::new(db));
        Ok(id)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_sibench_adapters_SlateDbStore_nativePut(
    env: JNIEnv,
    _: JClass,
    id: jlong,
    key: JByteArray,
    value: JByteArray,
) {
    call(env, |env| {
        store(id)?.put(&bytes(env, &key)?, &bytes(env, &value)?)
    })
}
#[no_mangle]
pub extern "system" fn Java_org_sibench_adapters_SlateDbStore_nativePutAll(
    env: JNIEnv,
    _: JClass,
    id: jlong,
    keys: JObjectArray,
    values: JObjectArray,
) {
    call(env, |env| {
        let count = env.get_array_length(&keys).map_err(error)?;
        if env.get_array_length(&values).map_err(error)? != count {
            return Err("Baseline keys and values differ in length".into());
        }
        let mut entries = Vec::with_capacity(count as usize);
        for i in 0..count {
            let key = JByteArray::from(env.get_object_array_element(&keys, i).map_err(error)?);
            let value = JByteArray::from(env.get_object_array_element(&values, i).map_err(error)?);
            entries.push((bytes(env, &key)?, bytes(env, &value)?));
            env.delete_local_ref(key).map_err(error)?;
            env.delete_local_ref(value).map_err(error)?;
        }
        store(id)?.put_all(entries)
    })
}
#[no_mangle]
pub extern "system" fn Java_org_sibench_adapters_SlateDbStore_nativeGet(
    env: JNIEnv,
    _: JClass,
    id: jlong,
    key: JByteArray,
) -> jbyteArray {
    call(env, |env| match store(id)?.get(&bytes(env, &key)?)? {
        Some(v) => env
            .byte_array_from_slice(&v)
            .map(|v| v.into_raw())
            .map_err(error),
        None => Ok(std::ptr::null_mut()),
    })
}
#[no_mangle]
pub extern "system" fn Java_org_sibench_adapters_SlateDbStore_nativeFlush(
    env: JNIEnv,
    _: JClass,
    id: jlong,
) {
    call(env, |_| store(id)?.flush())
}
#[no_mangle]
pub extern "system" fn Java_org_sibench_adapters_SlateDbStore_nativeRequests(
    env: JNIEnv,
    _: JClass,
    id: jlong,
) -> jstring {
    call(env, |env| {
        env.new_string(store(id)?.requests.text())
            .map(|s| s.into_raw())
            .map_err(error)
    })
}
#[no_mangle]
pub extern "system" fn Java_org_sibench_adapters_SlateDbStore_nativeClose(
    env: JNIEnv,
    _: JClass,
    id: jlong,
) {
    call(env, |_| {
        let value = stores()
            .write()
            .map_err(|_| "Native state lock poisoned")?
            .remove(&id);
        if let Some(value) = value {
            value.close()?;
        }
        Ok(())
    })
}
#[no_mangle]
pub extern "system" fn Java_org_sibench_adapters_SlateDbStore_nativeCursor(
    env: JNIEnv,
    _: JClass,
    id: jlong,
    lower: JByteArray,
    upper: JByteArray,
) -> jlong {
    call(env, |env| {
        let cursor = Cursor::new(
            store(id)?,
            optional_bytes(env, &lower)?,
            optional_bytes(env, &upper)?,
        )?;
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        lock(cursors(id))?.insert(id, Arc::new(Mutex::new(cursor)));
        Ok(id)
    })
}
#[no_mangle]
pub extern "system" fn Java_org_sibench_adapters_SlateDbStore_nativeMove(
    env: JNIEnv,
    _: JClass,
    id: jlong,
    operation: jint,
    key: JByteArray,
) {
    call(env, |env| {
        let cursor = cursor(id)?;
        let mut value = lock(&cursor)?;
        match operation {
            0 => value.seek(Bytes::from(bytes(env, &key)?)),
            1 => value.position(Some(Bytes::from(bytes(env, &key)?)), true, true),
            2 => value.position(None, true, true),
            3 => value.step(false),
            4 => value.step(true),
            _ => Err("Unknown cursor operation".into()),
        }
    })
}
#[no_mangle]
pub extern "system" fn Java_org_sibench_adapters_SlateDbStore_nativeValid(
    env: JNIEnv,
    _: JClass,
    id: jlong,
) -> jboolean {
    call(env, |_| {
        let cursor = cursor(id)?;
        let value = lock(&cursor)?;
        Ok(u8::from(value.current.is_some()))
    })
}
#[no_mangle]
pub extern "system" fn Java_org_sibench_adapters_SlateDbStore_nativeEntry(
    env: JNIEnv,
    _: JClass,
    id: jlong,
    key: jboolean,
) -> jbyteArray {
    call(env, |env| {
        let cursor = cursor(id)?;
        let value = lock(&cursor)?;
        let pair = value.current.as_ref().ok_or("Cursor is not on an entry")?;
        env.byte_array_from_slice(if key != 0 { &pair.0 } else { &pair.1 })
            .map(|v| v.into_raw())
            .map_err(error)
    })
}
#[no_mangle]
pub extern "system" fn Java_org_sibench_adapters_SlateDbStore_nativeCloseCursor(
    env: JNIEnv,
    _: JClass,
    id: jlong,
) {
    call(env, |_| {
        lock(cursors(id))?.remove(&id);
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::config::{FlushOptions, FlushType};
    use slatedb::object_store::memory::InMemory;

    fn temporary(name: &str) -> std::path::PathBuf {
        let path =
            std::env::temp_dir().join(format!("sibench-slatedb-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        path
    }

    /// Counts remote GETs of compacted SSTs only; background manifest and WAL reads are not lookups.
    #[derive(Debug)]
    struct SstGets {
        inner: Arc<dyn ObjectStore>,
        count: AtomicU64,
    }

    impl std::fmt::Display for SstGets {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("SstGets")
        }
    }

    #[async_trait]
    impl ObjectStore for SstGets {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> slatedb::object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> slatedb::object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> slatedb::object_store::Result<GetResult> {
            if !options.head && location.as_ref().contains("/compacted/") {
                self.count.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, slatedb::object_store::Result<Path>>,
        ) -> BoxStream<'static, slatedb::object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, slatedb::object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> slatedb::object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> slatedb::object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn sst_store() -> Arc<SstGets> {
        Arc::new(SstGets {
            inner: Arc::new(InMemory::new()),
            count: AtomicU64::new(0),
        })
    }

    fn reads(objects: &SstGets) -> u64 {
        objects.count.load(Ordering::Relaxed)
    }

    #[test]
    fn compacted_ssts_are_read_from_the_local_mirror_and_remote_requests_are_counted() {
        let gets = sst_store();
        let objects: Arc<dyn ObjectStore> = gets.clone();
        let mirror = temporary("mirror");
        let store = Store::open(objects.clone(), "mirror-test", Some(&mirror), true).unwrap();
        store.put(b"key", b"value").unwrap();
        store
            .runtime
            .block_on(store.db.flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            }))
            .unwrap();
        let text = store.requests.text();
        let write: Vec<u64> = text
            .lines()
            .find(|l| l.starts_with("write "))
            .unwrap()
            .split(' ')
            .skip(1)
            .map(|v| v.parse().unwrap())
            .collect();
        assert_eq!(write.len(), 36);
        assert!(
            write[0] > 0 && write[2] > 0,
            "Remote uploads must be counted with their bytes"
        );
        store.close().unwrap();
        drop(store);
        let reopened = Store::open(objects, "mirror-test", Some(&mirror), false).unwrap();
        let opened = reads(&gets);
        let mut cursor = Cursor::new(Arc::new(reopened), None, None).unwrap();
        cursor.seek(Bytes::from_static(b"key")).unwrap();
        assert_eq!(cursor.current.as_ref().unwrap().1.as_ref(), b"value");
        assert_eq!(
            reads(&gets),
            opened,
            "A mirrored SST must not be fetched remotely"
        );
        cursor.store.close().unwrap();
        let _ = std::fs::remove_dir_all(&mirror);
    }

    #[test]
    fn a_baseline_batch_is_one_visible_write() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = Store::open(objects, "batch-test", None, true).unwrap();
        store
            .put_all(vec![
                (b"a".to_vec(), b"A".to_vec()),
                (b"b".to_vec(), b"B".to_vec()),
            ])
            .unwrap();
        assert_eq!(store.get(b"a").unwrap().unwrap().as_ref(), b"A");
        assert_eq!(store.get(b"b").unwrap().unwrap().as_ref(), b"B");
        store.close().unwrap();
    }

    #[test]
    fn virtual_hosted_endpoints_name_the_bucket_once() {
        assert_eq!(
            s3_endpoint("https://cos.example.com", "b-1", true),
            "https://b-1.cos.example.com"
        );
        assert_eq!(
            s3_endpoint("https://b-1.cos.example.com", "b-1", true),
            "https://b-1.cos.example.com"
        );
        assert_eq!(
            s3_endpoint("http://127.0.0.1:8333", "b-1", false),
            "http://127.0.0.1:8333"
        );
    }

    #[test]
    fn repeated_cursor_seeks_reuse_cached_blocks() {
        let gets = sst_store();
        let objects: Arc<dyn ObjectStore> = gets.clone();
        let store = Store::open(objects.clone(), "cache-test", None, true).unwrap();
        for key in 0..20_000u32 {
            store.put(&key.to_be_bytes(), &[7; 64]).unwrap();
        }
        store
            .runtime
            .block_on(store.db.flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            }))
            .unwrap();
        store.close().unwrap();
        drop(store);
        let store = Arc::new(Store::open(objects, "cache-test", None, false).unwrap());
        let seek = |key: u32| {
            let mut cursor = Cursor::new(store.clone(), None, None).unwrap();
            cursor
                .seek(Bytes::copy_from_slice(&key.to_be_bytes()))
                .unwrap();
            assert_eq!(
                cursor.current.as_ref().unwrap().0.as_ref(),
                key.to_be_bytes()
            );
        };
        seek(10_000);
        let first = reads(&gets);
        assert!(first > 0, "The first seek fetches its block");
        seek(10_000);
        // SlateDB's scan default (cache_blocks = false) fetches this data block again.
        assert_eq!(
            reads(&gets),
            first,
            "A repeated seek must hit the block cache"
        );
        store.close().unwrap();
    }

    #[test]
    fn exact_bytes_bounds_directions_and_snapshot_are_independent_of_later_writes() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mirror = temporary("native");
        let store =
            Arc::new(Store::open(objects.clone(), "native-test", Some(&mirror), true).unwrap());
        for (key, value) in [
            (b"a".as_slice(), b"old".as_slice()),
            (b"b", b"B"),
            (b"d", b"D"),
            (b"\x80", b"high"),
            (b"\xff", b"last"),
        ] {
            store.put(key, value).unwrap();
        }
        assert_eq!(store.get(b"a").unwrap().unwrap().as_ref(), b"old");
        let mut cursor = Cursor::new(
            store.clone(),
            Some(Bytes::from_static(b"b")),
            Some(Bytes::from_static(b"\xff")),
        )
        .unwrap();
        store.put(b"b", b"replaced").unwrap();
        cursor.seek(Bytes::from_static(b"a")).unwrap();
        assert_eq!(
            cursor.current.as_ref().unwrap(),
            &(Bytes::from_static(b"b"), Bytes::from_static(b"B"))
        );
        cursor.seek(Bytes::from_static(b"c")).unwrap();
        assert_eq!(cursor.current.as_ref().unwrap().0.as_ref(), b"d");
        cursor.step(true).unwrap();
        assert_eq!(cursor.current.as_ref().unwrap().0.as_ref(), b"b");
        cursor.position(None, true, true).unwrap();
        assert_eq!(cursor.current.as_ref().unwrap().0.as_ref(), b"\x80");
        cursor.step(true).unwrap();
        assert_eq!(cursor.current.as_ref().unwrap().0.as_ref(), b"d");
        cursor
            .position(Some(Bytes::from_static(b"c")), true, true)
            .unwrap();
        assert_eq!(cursor.current.as_ref().unwrap().0.as_ref(), b"b");
        cursor.seek(Bytes::from_static(b"\xff")).unwrap();
        assert!(cursor.current.is_none());
        cursor
            .position(Some(Bytes::from_static(b"a")), true, true)
            .unwrap();
        assert!(cursor.current.is_none());
        drop(cursor);
        assert!(Store::open(
            objects.clone(),
            "native-test",
            Some(&temporary("second")),
            true
        )
        .is_err());
        store.flush().unwrap();
        store.close().unwrap();
        drop(store);
        let reopened = Store::open(objects, "native-test", Some(&mirror), false).unwrap();
        assert_eq!(reopened.get(b"b").unwrap().unwrap().as_ref(), b"replaced");
        reopened.close().unwrap();
    }
}
