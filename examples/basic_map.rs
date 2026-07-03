use prolly::{Config, MemStore, Prolly};

fn main() -> Result<(), prolly::Error> {
    let prolly = Prolly::new(MemStore::new(), Config::default());

    let tree = prolly.create();
    let tree = prolly.put(&tree, b"user:001".to_vec(), b"Ada".to_vec())?;
    let tree = prolly.put(&tree, b"user:002".to_vec(), b"Grace".to_vec())?;
    let tree = prolly.put(&tree, b"user:003".to_vec(), b"Linus".to_vec())?;

    assert_eq!(prolly.get(&tree, b"user:001")?, Some(b"Ada".to_vec()));

    let tree = prolly.delete(&tree, b"user:003")?;
    assert_eq!(prolly.get(&tree, b"user:003")?, None);

    let users = prolly
        .range(&tree, b"user:", Some(b"user;"))?
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(
        users,
        vec![
            (b"user:001".to_vec(), b"Ada".to_vec()),
            (b"user:002".to_vec(), b"Grace".to_vec()),
        ]
    );

    println!("{} users in range", users.len());
    Ok(())
}
