use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use instrument_fs::{adapter::glommio::InstrumentedDirectory, Event, Instrument};

pub fn test_dir(subdir_name: impl AsRef<Path>) -> PathBuf {
    // TODO use target or something else. CARGO_TARGET_DIR is not set automatically
    //    it is only read if it is available
    let mut path = PathBuf::from_str(env!("CARGO_MANIFEST_DIR")).unwrap();
    path.push("test_data");
    path.push(subdir_name.as_ref());

    // consume value so there is no warning about must_use
    // glommio's directory doesnt have rm method, so let it be as it is
    fs::remove_dir_all(&path).ok();

    // and there is no create dir all in glommio too
    fs::create_dir_all(&path).expect("failed to create test dir");

    path
}

pub async fn test_dir_open<P: AsRef<Path>, I: Instrument + Clone>(
    subdir_name: P,
    instrument: I,
) -> (InstrumentedDirectory<I>, PathBuf) {
    let path = test_dir(subdir_name);

    instrument
        .apply_event(Event::AddTrustedDir(path.clone()))
        .unwrap();

    let dir = InstrumentedDirectory::open(&path, instrument)
        .await
        .expect("failed to create wal dir for test directory");

    dir.sync().await.expect("failed to sync wal dir");

    (dir, path)
}
