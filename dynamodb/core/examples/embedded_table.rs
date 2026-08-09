use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};

use prolly::{Config, MemStore, SyncStoreAsAsync};
use prolly_dynamodb_core::{AttributeValue, Database, DynamoNumber, Item, KeyAttribute, KeyKind};

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = futures_util::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn key(account: &str, sequence: &str) -> Item {
    BTreeMap::from([
        ("account".into(), AttributeValue::S(account.into())),
        (
            "sequence".into(),
            AttributeValue::N(DynamoNumber::parse(sequence).expect("valid example number")),
        ),
    ])
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    block_on(async {
        let store = SyncStoreAsAsync::new(Arc::new(MemStore::new()));
        let database = Database::open(store, Config::default()).await?;
        database
            .create_table(
                "Orders",
                KeyAttribute {
                    name: "account".into(),
                    kind: KeyKind::String,
                },
                Some(KeyAttribute {
                    name: "sequence".into(),
                    kind: KeyKind::Number,
                }),
            )
            .await?;

        let mut order = key("acct-1", "1.20");
        order.insert("status".into(), AttributeValue::S("OPEN".into()));
        let first = database.put_item("Orders", order, None).await?;
        let first_version = match first {
            prolly::VersionedMapUpdate::Applied { current, .. } => current.id,
            other => return Err(format!("unexpected initial update {other:?}").into()),
        };

        let mut updated = key("acct-1", "1.2");
        updated.insert("status".into(), AttributeValue::S("CLOSED".into()));
        database.put_item("Orders", updated, None).await?;

        let historical = database
            .get_item_at("Orders", &first_version, &key("acct-1", "1.200"))
            .await?
            .ok_or("historical item is missing")?;
        assert_eq!(
            historical.get("status"),
            Some(&AttributeValue::S("OPEN".into()))
        );
        println!("first version: {first_version}");
        Ok(())
    })
}
