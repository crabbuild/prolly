use std::env;
use std::error::Error;
use std::io;

use prolly::{AsyncProlly, Config, RemoteProllyStore};
use prolly_store_postgres::PostgresBackend;

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let operation = required(&mut args, "operation")?;
    let database_url = required(&mut args, "database URL")?;
    let root = required(&mut args, "root name")?.into_bytes();
    let key = required(&mut args, "key")?.into_bytes();
    let value = required(&mut args, "value")?.into_bytes();
    if args.next().is_some() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "too many arguments").into());
    }

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?
        .block_on(async {
            let backend = PostgresBackend::connect(&database_url).await?;
            backend.initialize_schema().await?;
            let prolly = AsyncProlly::new(RemoteProllyStore::new(backend), Config::default());
            match operation.as_str() {
                "write" => {
                    let tree = prolly.put(&prolly.create(), key, value).await?;
                    prolly.publish_named_root(&root, &tree).await?;
                }
                "verify" => {
                    let tree = prolly.load_named_root(&root).await?.ok_or_else(|| {
                        io::Error::new(io::ErrorKind::NotFound, "named root is missing")
                    })?;
                    let actual = prolly.get(&tree, &key).await?.ok_or_else(|| {
                        io::Error::new(io::ErrorKind::NotFound, "tree key is missing")
                    })?;
                    if actual != value {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "tree value differs",
                        )
                        .into());
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
            Ok::<(), Box<dyn Error>>(())
        })
}

fn required(args: &mut impl Iterator<Item = String>, name: &str) -> io::Result<String> {
    args.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("missing {name} argument"),
        )
    })
}
