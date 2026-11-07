//! JNI byte storage only. Java owns index keys, versions, Trace semantics and result validation.
use bytes::Bytes;
use futures::TryStreamExt;
use jni::{
    objects::{JByteArray, JClass, JString},
    sys::{jboolean, jbyteArray, jint, jlong, jstring},
    JNIEnv,
};
use slatedb::{
    config::{ScanOptions, Settings},
    object_store::{
        aws::AmazonS3Builder, path::Path, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
    },
    Db, DbIterator, DbSnapshot, IterationOrder,
};
use std::{
    collections::HashMap,
    ops::Bound,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, Mutex, OnceLock,
    },
};
use tokio::runtime::Runtime;

type Result<T> = std::result::Result<T, String>;
static NEXT: AtomicI64 = AtomicI64::new(1);
static STORES: OnceLock<Mutex<HashMap<i64, Arc<Store>>>> = OnceLock::new();
static CURSORS: OnceLock<Mutex<HashMap<i64, Arc<Mutex<Cursor>>>>> = OnceLock::new();
const OWNER: &[u8] = b"SIBench SlateDB byte store\n";

struct Store {
    runtime: Runtime,
    db: Db,
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

fn stores() -> &'static Mutex<HashMap<i64, Arc<Store>>> {
    STORES.get_or_init(Default::default)
}
fn cursors() -> &'static Mutex<HashMap<i64, Arc<Mutex<Cursor>>>> {
    CURSORS.get_or_init(Default::default)
}
fn lock<T>(value: &Mutex<T>) -> Result<std::sync::MutexGuard<'_, T>> {
    value
        .lock()
        .map_err(|_| "Native state lock poisoned".into())
}
fn store(id: i64) -> Result<Arc<Store>> {
    lock(stores())?
        .get(&id)
        .cloned()
        .ok_or("SlateDB store is closed".into())
}
fn cursor(id: i64) -> Result<Arc<Mutex<Cursor>>> {
    lock(cursors())?
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
    fn open(objects: Arc<dyn ObjectStore>, prefix: &str, create: bool) -> Result<Self> {
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
            Db::builder(prefix, objects)
                .with_settings(Settings {
                    object_store_max_retries: Some(3),
                    ..Settings::default()
                })
                .build()
                .await
                .map_err(error)
        })?;
        Ok(Self { runtime, db })
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.runtime
            .block_on(self.db.put(key, value))
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
        let options = ScanOptions::default().with_order(if reverse {
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
    create: jboolean,
) -> jlong {
    call(env, |env| {
        let endpoint = string(env, &endpoint)?;
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(string(env, &bucket)?)
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
            create != 0,
        )?;
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        lock(stores())?.insert(id, Arc::new(db));
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
pub extern "system" fn Java_org_sibench_adapters_SlateDbStore_nativeClose(
    env: JNIEnv,
    _: JClass,
    id: jlong,
) {
    call(env, |_| {
        let value = lock(stores())?.remove(&id);
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
        lock(cursors())?.insert(id, Arc::new(Mutex::new(cursor)));
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
        lock(cursors())?.remove(&id);
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;

    #[test]
    fn exact_bytes_bounds_directions_and_snapshot_are_independent_of_later_writes() {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = Arc::new(Store::open(objects.clone(), "native-test", true).unwrap());
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
        assert!(Store::open(objects.clone(), "native-test", true).is_err());
        store.flush().unwrap();
        store.close().unwrap();
        let reopened = Store::open(objects, "native-test", false).unwrap();
        assert_eq!(reopened.get(b"b").unwrap().unwrap().as_ref(), b"replaced");
        reopened.close().unwrap();
    }
}
