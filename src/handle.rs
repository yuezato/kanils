extern crate cannyls;
use cannyls::lump::{LumpData, LumpId};
use cannyls::nvm::FileNvm;
use cannyls::storage::{JournalSnapshot, Storage, StorageBuilder};

use fibers::sync::oneshot;
use fibers_http_server::metrics::MetricsHandler;
use fibers_http_server::ServerBuilder;
use futures::Future;
use std::path::Path;
use std::str;

fn lumpdata_to_string(data: &LumpData) -> String {
    String::from_utf8(data.as_bytes().to_vec()).expect("should succeed")
}

pub struct StorageHandle {
    storage: Storage<FileNvm>,
    tx: Option<oneshot::Sender<()>>,
    port: Option<u16>,
}

impl StorageHandle {
    /// CannyLSの集めているメトリクスデータを
    /// 改行区切りの文字列として返す
    pub fn metrics(&self) -> Option<String> {
        let mut lock = prometrics::default_gatherer().try_lock();

        if let Ok(ref mut mutex) = lock {
            return Some(mutex.gather().to_text());
        } else {
            return None;
        }
    }

    /// CannyLSの集めているメトリクスデータを
    /// Prometheusがpullするためのサーバを起動する。
    ///
    /// 引数 `port` は、メトリクスサーバをbindするためのポート番号として用いられる。
    pub fn start_metrics_server(&mut self, port: u16) {
        if self.tx.is_some() {
            println!(
                "The metrics server is already running with the port {}.",
                self.port.unwrap()
            );
            println!(
                "Please visit http://localhost:{}/metrics .",
                self.port.unwrap()
            );
            return;
        }

        let (tx, rx) = oneshot::channel();
        let addr = format!("0.0.0.0:{}", port).parse().unwrap();
        let mut builder = ServerBuilder::new(addr);
        builder.add_handler(MetricsHandler).unwrap();

        let server = builder.finish(fibers_global::handle());
        fibers_global::spawn(server.select2(rx).map(|_| ()).map_err(|_| ()));

        self.tx = Some(tx);
        self.port = Some(port);

        println!("Start our metrics server on the port {}.", port);
        println!(
            "Please visit http://localhost:{}/metrics .",
            self.port.unwrap()
        );
    }

    /// `start_metrics_server` で起動したメトリクスサーバを
    /// 終了させる。
    pub fn finish_metrics_server(&mut self) {
        if let Some(tx) = self.tx.take() {
            println!("finishing the metrics server...");
            tx.send(()).unwrap();
            println!("done!");
        } else {
            println!("The metrics server is not running");
        }
    }

    pub fn new(storage: Storage<FileNvm>) -> Self {
        StorageHandle {
            storage,
            tx: None,
            port: None,
        }
    }

    pub fn create<T: AsRef<Path>>(path: T) -> Self {
        let nvm = track_try_unwrap!(FileNvm::open(path));
        let storage = track_try_unwrap!(StorageBuilder::new().open(nvm));
        StorageHandle {
            storage,
            tx: None,
            port: None,
        }
    }

    /// `u128型`の値`key`からLumpIDを生成し、文字列`value`をLumpDataに変換し
    /// これらの組からなるLumpをPUTする。
    ///
    /// 返り値はそれぞれ次を意味する:
    /// - Ok(true): PUTに成功した。特に「新規書き込み」が行われた。
    /// - Ok(false): PUTに成功した。特に「上書き」が行われた。
    /// - Err: CannyLSのレベルでエラーが発生した。
    pub fn put_str(&mut self, key: u128, value: &str) -> Result<bool, cannyls::Error> {
        let lump_id = LumpId::new(key);
        let lump_data =
            track_try_unwrap!(self.storage.allocate_lump_data_with_bytes(value.as_bytes()));
        self.storage.put(&lump_id, &lump_data)
    }

    /// `u128型`の値`key`と文字列`value`からLumpを作りPUTする。
    ///
    /// 内部で`put_str`を呼び出し、新規書き込み `new` であったか、上書き `overwrite` であったかを区別する。
    pub fn put(&mut self, key: u128, value: &str) {
        let result = track_try_unwrap!(self.put_str(key, value));

        if result {
            println!("[new] put key={}, value={}", key, value);
        } else {
            println!("[overwrite] put key={}, value={}", key, value);
        }
    }

    /// `u128型`の値`key`からLumpIDを生成し、そのLumpIDに紐付いたデータを取得する。
    ///
    /// 返り値はそれぞれ次を意味する:
    /// - `Ok(Some(string))`: `string`がストアされていた。
    /// - `Ok(None)`: `key`のLumpIDを持つLumpは存在しなかった。
    /// - `None`: CannyLSのレベルでエラーが発生した。
    pub fn get_string(&mut self, key: u128) -> Result<Option<String>, cannyls::Error> {
        let lump_id = LumpId::new(key);
        self.storage
            .get(&lump_id)
            .map(|s| s.map(|s| lumpdata_to_string(&s)))
    }
    /// `u128型`の値`key`からLumpIDを生成し、そのLumpIDに紐付いたデータを出力する。
    pub fn get(&mut self, key: u128) {
        let result = track_try_unwrap!(self.get_string(key));
        if let Some(string) = result {
            println!("get => {:?}", string);
        } else {
            println!("no entry for the key {:?}", key);
        }
    }

    /// `u128型`の値`key`からLumpIDを生成し、そのLumpIDに紐付いたデータを削除する。
    ///
    /// 返り値はそれぞれ次を意味する:
    /// - `Ok(true)`: `key`のLumpIDを持つLumpが存在しており、削除された。
    /// - `Ok(false)`: `key`のLumpIDを持つLumpは存在しなかった。
    /// - `None`: CannyLSのレベルでエラーが発生した。
    pub fn delete_key(&mut self, key: u128) -> Result<bool, cannyls::Error> {
        let lump_id = LumpId::new(key);
        self.storage.delete(&lump_id)
    }
    /// `u128型`の値`key`からLumpIDを生成し、そのLumpIDに紐付いたデータを削除し、結果を出力する。
    pub fn delete(&mut self, key: u128) {
        let result = track_try_unwrap!(self.delete_key(key));
        println!("delete result => {:?}", result);
    }

    pub fn journal_info(&mut self) -> Result<JournalSnapshot, cannyls::Error> {
        self.storage.journal_snapshot()
    }

    pub fn print_journal_info(&mut self) {
        let snapshot = track_try_unwrap!(self.journal_info());

        println!(
            "journal [unreleased head] position = {}",
            snapshot.unreleased_head
        );
        println!("journal [head] position = {}", snapshot.head);
        println!("journal [tail] position = {}", snapshot.tail);

        if snapshot.entries.is_empty() {
            println!("there are no journal entries");
        } else {
            println!("<journal entries>");
            for e in snapshot.entries {
                println!("{:?}", e);
            }
            println!("</journal entries>");
        }
    }

    pub fn journal_gc(&mut self) {
        println!("run journal full GC ...");
        track_try_unwrap!(self.storage.journal_sync());
        let result = self.storage.journal_gc();
        if let Err(error) = result {
            panic!("journal_gc failed with the error {:?}", error);
        } else {
            println!("journal full GC succeeded!");
        }
    }

    pub fn all_keys(&mut self) -> Vec<LumpId> {
        self.storage.list()
    }

    pub fn print_list_of_lumpids(&mut self) {
        let ids = self.storage.list();
        if ids.is_empty() {
            println!("there are no lumps");
        } else {
            println!("<lumpid list>");
            for lumpid in ids {
                println!("{:?}", lumpid);
            }
            println!("</lumpid list>");
        }
    }

    pub fn print_all_key_value_pairs(&mut self) {
        let ids = self.storage.list();
        if ids.is_empty() {
            println!("there are no lumps");
        } else {
            let result = ids
                .iter()
                .map(|key| {
                    (
                        key,
                        lumpdata_to_string(&self.storage.get(key).unwrap().unwrap()),
                    )
                }).collect::<Vec<_>>();
            println!("<lump list>");
            for lump in result {
                println!("{:?}", lump);
            }
            println!("</lump list>");
        }
    }

    pub fn print_header_info(&mut self) {
        let header = self.storage.header();
        println!("header =>");
        println!("  major version = {}", header.major_version);
        println!("  minor version = {}", header.minor_version);
        let block_size_u64 = u64::from(header.block_size.as_u16());
        println!("  block size = {}", block_size_u64);
        println!("  uuid = {}", header.instance_uuid);
        println!("  journal region size = {}", header.journal_region_size);
        println!("    journal header size = {}", block_size_u64);
        println!(
            "    journal record size = {}",
            header.journal_region_size - block_size_u64
        );
        println!("  data region size = {}", header.data_region_size);
        println!("  storage header size => {}", header.region_size());
        println!("  storage total size = {}", header.storage_size());
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use trackable::result::TestResult;

    use super::*;
    use handle::StorageHandle;

    macro_rules! track_io {
        ($expr:expr) => {
            $expr.map_err(|e: ::std::io::Error| track!(cannyls::Error::from(e)))
        };
    }

    #[test]
    fn overwrite_works() -> TestResult {
        let dir = track_io!(TempDir::new("cannyls_test"))?;
        let path = dir.path().join("test.lusf");

        let nvm = track_try_unwrap!(FileNvm::create(path, 4_000_000));
        let storage = track_try_unwrap!(Storage::create(nvm));
        let mut handle = StorageHandle::new(storage);

        assert!(handle.put_str(0, "hoge").is_ok());
        assert!(handle.put_str(0, "bar").is_ok());
        assert_eq!(handle.get_string(0)?.unwrap(), "bar".to_owned());

        Ok(())
    }

    #[test]
    fn delete_works() -> TestResult {
        let dir = track_io!(TempDir::new("cannyls_test"))?;
        let path = dir.path().join("test.lusf");

        let nvm = track_try_unwrap!(FileNvm::create(path, 4_000_000));
        let storage = track_try_unwrap!(Storage::create(nvm));
        let mut handle = StorageHandle::new(storage);

        assert!(handle.put_str(0, "hoge").is_ok());
        assert!(handle.delete_key(0)?, true);
        assert!(handle.get_string(0)?.is_none());

        Ok(())
    }
}
