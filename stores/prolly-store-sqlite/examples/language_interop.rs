use std::env;
use std::error::Error;
use std::io;
use std::sync::Arc;

use prolly::{Config, Prolly};
use prolly_store_sqlite::SqliteStore;

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let operation = required(&mut args, "operation")?;
    let path = required(&mut args, "database path")?;
    let root = required(&mut args, "root name")?.into_bytes();
    let key = required(&mut args, "key")?.into_bytes();
    let value = required(&mut args, "value")?.into_bytes();
    if args.next().is_some() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "too many arguments").into());
    }

    let store = Arc::new(SqliteStore::open(path)?);
    let prolly = Prolly::new(store, Config::default());
    match operation.as_str() {
        "write" => {
            let tree = prolly.put(&prolly.create(), key, value)?;
            prolly.publish_named_root(&root, &tree)?;
        }
        "verify" => {
            let tree = prolly
                .load_named_root(&root)?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "named root is missing"))?;
            let actual = prolly
                .get(&tree, &key)?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "tree key is missing"))?;
            if actual != value {
                return Err(
                    io::Error::new(io::ErrorKind::InvalidData, "tree value differs").into(),
                );
            }
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "operation must be write or verify",
            )
            .into());
        }
    }
    Ok(())
}

fn required(args: &mut impl Iterator<Item = String>, name: &str) -> io::Result<String> {
    args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("missing {name} argument"),
        )
    })
}
