use prolly::{Config, Prolly};
use prolly_store_redb::RedbStore;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = RedbStore::open("./data/app.prolly.redb")?;
    let prolly = Prolly::new(store, Config::default());

    let tree = prolly.put(
        &prolly.create(),
        b"project/name".to_vec(),
        b"CrabDB".to_vec(),
    )?;
    prolly.publish_named_root(b"main", &tree)?;

    let loaded = prolly.load_named_root(b"main")?.expect("main root");
    assert_eq!(
        prolly.get(&loaded, b"project/name")?,
        Some(b"CrabDB".to_vec())
    );
    Ok(())
}
