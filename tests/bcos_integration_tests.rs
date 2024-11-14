#![cfg(feature = "integration-tests")]

//! # Naming convention
//!
//! Test names should begin with one of the following:
//! 1. txn_
//! 2. raw_
//! 3. misc_
//!
//! We make use of the convention to control the order of tests in CI, to allow
//! transactional and raw tests to coexist, since transactional requests have
//! requirements on the region boundaries.

mod common;
use common::{init, pd_addrs};
// use futures::prelude::*;
use rand::{seq::IteratorRandom, thread_rng, Rng};
use serial_test::serial;
// use slog::{o, Drain};
use std::{
    collections::{HashMap, HashSet},
    convert::TryInto,
    iter, println,
};
use tikv_client::{
    transaction::HeartbeatOption, Key, Result, Transaction, TransactionClient, TransactionOptions,
    Value,
};

use tokio::time::Instant;

// Parameters used in test
const NUM_PEOPLE: u32 = 100;
const NUM_TRANSFER: u32 = 100;

// Tests transactional get, put, delete, batch_get
#[tokio::test]
#[serial]
async fn txn_crud() -> Result<()> {
    init().await?;

    let client = TransactionClient::new(pd_addrs()).await?;
    let mut txn = client.begin_optimistic().await?;

    // Get non-existent keys
    assert!(txn.get("foo".to_owned()).await?.is_none());

    // batch_get do not return non-existent entries
    assert_eq!(
        txn.batch_get(vec!["foo".to_owned(), "bar".to_owned()])
            .await?
            .count(),
        0
    );

    txn.put("foo".to_owned(), "bar".to_owned()).await?;
    txn.put("bar".to_owned(), "foo".to_owned()).await?;
    // Read buffered values
    assert_eq!(
        txn.get("foo".to_owned()).await?,
        Some("bar".to_owned().into())
    );
    let batch_get_res: HashMap<Key, Value> = txn
        .batch_get(vec!["foo".to_owned(), "bar".to_owned()])
        .await?
        .map(|pair| (pair.0, pair.1))
        .collect();
    assert_eq!(
        batch_get_res.get(&Key::from("foo".to_owned())),
        Some(Value::from("bar".to_owned())).as_ref()
    );
    assert_eq!(
        batch_get_res.get(&Key::from("bar".to_owned())),
        Some(Value::from("foo".to_owned())).as_ref()
    );
    // txn.commit().await?;
    txn.prewrite_primary(None).await?;
    let commit_ts = txn.commit_primary().await?;
    txn.commit_secondary(commit_ts).await;

    // Read from TiKV then update and delete
    let mut txn = client.begin_optimistic().await?;
    assert_eq!(
        txn.get("foo".to_owned()).await?,
        Some("bar".to_owned().into())
    );
    let batch_get_res: HashMap<Key, Value> = txn
        .batch_get(vec!["foo".to_owned(), "bar".to_owned()])
        .await?
        .map(|pair| (pair.0, pair.1))
        .collect();
    assert_eq!(
        batch_get_res.get(&Key::from("foo".to_owned())),
        Some(Value::from("bar".to_owned())).as_ref()
    );
    assert_eq!(
        batch_get_res.get(&Key::from("bar".to_owned())),
        Some(Value::from("foo".to_owned())).as_ref()
    );
    txn.put("foo".to_owned(), "foo".to_owned()).await?;
    txn.delete("bar".to_owned()).await?;
    // txn.commit().await?;
    txn.prewrite_primary(None).await?;
    let commit_ts = txn.commit_primary().await?;
    txn.commit_secondary(commit_ts).await;

    // Read again from TiKV
    let mut snapshot = client.snapshot(
        client.current_timestamp().await?,
        // TODO needed because pessimistic does not check locks (#235)
        TransactionOptions::new_optimistic(),
    );
    let batch_get_res: HashMap<Key, Value> = snapshot
        .batch_get(vec!["foo".to_owned(), "bar".to_owned()])
        .await?
        .map(|pair| (pair.0, pair.1))
        .collect();
    assert_eq!(
        batch_get_res.get(&Key::from("foo".to_owned())),
        Some(Value::from("foo".to_owned())).as_ref()
    );
    assert_eq!(batch_get_res.get(&Key::from("bar".to_owned())), None);
    Ok(())
}

// Tests transactional insert and delete-your-writes cases
#[tokio::test]
#[serial]
async fn txn_insert_duplicate_keys() -> Result<()> {
    init().await?;

    let client = TransactionClient::new(pd_addrs()).await?;
    // Initialize TiKV store with {foo => bar}
    let mut txn = client.begin_optimistic().await?;
    txn.put("foo".to_owned(), "bar".to_owned()).await?;
    // txn.commit().await?;
    txn.prewrite_primary(None).await?;
    let commit_ts = txn.commit_primary().await?;
    txn.commit_secondary(commit_ts).await;

    // Try insert foo again
    let mut txn = client.begin_optimistic().await?;
    txn.insert("foo".to_owned(), "foo".to_owned()).await?;
    assert!(txn.commit().await.is_err());

    // Delete-your-writes
    let mut txn = client.begin_optimistic().await?;
    txn.insert("foo".to_owned(), "foo".to_owned()).await?;
    txn.delete("foo".to_owned()).await?;
    assert!(txn.commit().await.is_err());

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_pessimistic() -> Result<()> {
    init().await?;

    let client = TransactionClient::new(pd_addrs()).await?;
    let mut txn = client.begin_pessimistic().await?;
    txn.put("foo".to_owned(), "foo".to_owned()).await.unwrap();

    let ttl = txn.send_heart_beat().await.unwrap();
    assert!(ttl > 0);

    // txn.commit().await.unwrap();
    txn.prewrite_primary(None).await?;
    let commit_ts = txn.commit_primary().await?;
    txn.commit_secondary(commit_ts).await;

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_read() -> Result<()> {
    const NUM_BITS_TXN: u32 = 4;
    const NUM_BITS_KEY_PER_TXN: u32 = 4;
    let interval = 2u32.pow(32 - NUM_BITS_TXN - NUM_BITS_KEY_PER_TXN);
    let value = "large_value".repeat(10);

    init().await?;
    let client = TransactionClient::new(pd_addrs()).await?;

    for i in 0..2u32.pow(NUM_BITS_TXN) {
        let mut cur = i * 2u32.pow(32 - NUM_BITS_TXN);
        let keys = iter::repeat_with(|| {
            let v = cur;
            cur = cur.overflowing_add(interval).0;
            v
        })
        .map(|u| u.to_be_bytes().to_vec())
        .take(2usize.pow(NUM_BITS_KEY_PER_TXN))
        .collect::<Vec<_>>();
        let mut txn = client.begin_optimistic().await?;
        for (k, v) in keys.iter().zip(iter::repeat(value.clone())) {
            txn.put(k.clone(), v).await?;
        }
        // txn.commit().await?;
        txn.prewrite_primary(None).await?;
        let commit_ts = txn.commit_primary().await?;
        txn.commit_secondary(commit_ts).await;

        let mut txn = client.begin_optimistic().await?;
        let res = txn.batch_get(keys).await?;
        assert_eq!(res.count(), 2usize.pow(NUM_BITS_KEY_PER_TXN));
        txn.commit().await?;
    }
    // test scan
    let limit = 2u32.pow(NUM_BITS_KEY_PER_TXN + NUM_BITS_TXN + 2); // large enough
    let mut snapshot = client.snapshot(
        client.current_timestamp().await?,
        TransactionOptions::default(),
    );
    let res = snapshot.scan(vec![].., limit).await?;
    assert_eq!(res.count(), 2usize.pow(NUM_BITS_KEY_PER_TXN + NUM_BITS_TXN));

    // scan by small range and combine them
    let mut rng = thread_rng();
    let mut keys = gen_u32_keys(200, &mut rng)
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    keys.sort();

    let mut sum = 0;

    // empty key to key[0]
    let mut snapshot = client.snapshot(
        client.current_timestamp().await?,
        TransactionOptions::default(),
    );
    let res = snapshot.scan(vec![]..keys[0].clone(), limit).await?;
    sum += res.count();

    // key[i] .. key[i+1]
    for i in 0..keys.len() - 1 {
        let res = snapshot
            .scan(keys[i].clone()..keys[i + 1].clone(), limit)
            .await?;
        sum += res.count();
    }

    // keys[last] to unbounded
    let res = snapshot.scan(keys[keys.len() - 1].clone().., limit).await?;
    sum += res.count();

    assert_eq!(sum, 2usize.pow(NUM_BITS_KEY_PER_TXN + NUM_BITS_TXN));

    // test batch_get and batch_get_for_update
    const SKIP_BITS: u32 = 0; // do not retrieve all because there's a limit of message size
    let mut cur = 0u32;
    let keys = iter::repeat_with(|| {
        let v = cur;
        cur = cur.overflowing_add(interval * 2u32.pow(SKIP_BITS)).0;
        v
    })
    .map(|u| u.to_be_bytes().to_vec())
    .take(2usize.pow(NUM_BITS_KEY_PER_TXN + NUM_BITS_TXN - SKIP_BITS))
    .collect::<Vec<_>>();

    let mut txn = client.begin_pessimistic().await?;
    let res = txn.batch_get(keys.clone()).await?;
    assert_eq!(res.count(), keys.len());

    let res = txn.batch_get_for_update(keys.clone()).await?;
    assert_eq!(res.len(), keys.len());

    txn.commit().await?;
    Ok(())
}

// FIXME: the test is temporarily ingnored since it's easy to fail when scheduling is frequent.
#[tokio::test]
#[serial]
async fn txn_bank_transfer() -> Result<()> {
    init().await?;
    let client = TransactionClient::new(pd_addrs()).await?;
    let mut rng = thread_rng();
    let options = TransactionOptions::new_optimistic()
        .use_async_commit()
        .drop_check(tikv_client::CheckLevel::Warn);

    let people = gen_u32_keys(NUM_PEOPLE, &mut rng);
    let mut txn = client.begin_with_options(options.clone()).await?;
    let mut sum: u32 = 0;
    for person in &people {
        let init = rng.gen::<u8>() as u32;
        sum += init as u32;
        txn.put(person.clone(), init.to_be_bytes().to_vec()).await?;
    }
    // txn.commit().await?;
    txn.prewrite_primary(None).await?;
    let commit_ts = txn.commit_primary().await?;
    txn.commit_secondary(commit_ts).await;

    // transfer
    for _ in 0..NUM_TRANSFER {
        let mut txn = client.begin_with_options(options.clone()).await?;
        let chosen_people = people.iter().choose_multiple(&mut rng, 2);
        let alice = chosen_people[0];
        let mut alice_balance = get_txn_u32(&mut txn, alice.clone()).await?;
        let bob = chosen_people[1];
        let mut bob_balance = get_txn_u32(&mut txn, bob.clone()).await?;
        if alice_balance == 0 {
            txn.rollback().await?;
            continue;
        }
        let transfer = rng.gen_range(0..alice_balance);
        alice_balance -= transfer;
        bob_balance += transfer;
        txn.put(alice.clone(), alice_balance.to_be_bytes().to_vec())
            .await?;
        txn.put(bob.clone(), bob_balance.to_be_bytes().to_vec())
            .await?;
        // txn.commit().await?;
        txn.prewrite_primary(None).await?;
        let commit_ts = txn.commit_primary().await?;
        txn.commit_secondary(commit_ts).await;
    }

    // check
    let mut new_sum = 0;
    let mut txn = client.begin_optimistic().await?;
    for person in people.iter() {
        new_sum += get_txn_u32(&mut txn, person.clone()).await?;
    }
    assert_eq!(sum, new_sum);
    txn.commit().await?;
    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_pessimistic_rollback() -> Result<()> {
    init().await?;
    let client = TransactionClient::new_with_config(pd_addrs(), Default::default()).await?;
    let mut preload_txn = client.begin_optimistic().await?;
    let key1 = vec![1];
    let key2 = vec![2];
    let value = key1.clone();

    preload_txn.put(key1.clone(), value).await?;
    // preload_txn.commit().await?;
    preload_txn.prewrite_primary(None).await?;
    let commit_ts = preload_txn.commit_primary().await?;
    preload_txn.commit_secondary(commit_ts).await;

    for _ in 0..100 {
        let mut txn = client.begin_pessimistic().await?;
        let result = txn.get_for_update(key1.clone()).await;
        txn.rollback().await?;
        result?;
    }

    for _ in 0..100 {
        let mut txn = client.begin_pessimistic().await?;
        let result = txn
            .batch_get_for_update(vec![key1.clone(), key2.clone()])
            .await;
        txn.rollback().await?;
        let _ = result?;
    }

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_pessimistic_delete() -> Result<()> {
    init().await?;
    let client =
        TransactionClient::new_with_config(vec!["127.0.0.1:2379"], Default::default()).await?;

    // The transaction will lock the keys and must release the locks on commit,
    // even when values are not written to the DB.
    let mut txn = client.begin_pessimistic().await?;
    txn.put(vec![1], vec![42]).await?;
    txn.delete(vec![1]).await?;
    // FIXME
    //
    // A behavior change in TiKV 7.1 introduced in tikv/tikv#14293.
    //
    // An insert can return AlreadyExist error when the key exists.
    // We comment this line to allow the test to pass so that we can release v0.2
    // Should be addressed alter.
    // txn.insert(vec![2], vec![42]).await?;
    txn.delete(vec![2]).await?;
    txn.put(vec![3], vec![42]).await?;
    txn.prewrite_primary(None).await?;
    let commit_ts = txn.commit_primary().await?;
    txn.commit_secondary(commit_ts).await;

    // Check that the keys are not locked.
    let mut txn2 = client.begin_optimistic().await?;
    txn2.put(vec![1], vec![42]).await?;
    txn2.put(vec![2], vec![42]).await?;
    txn2.put(vec![3], vec![42]).await?;
    // txn2.commit().await?;
    txn2.prewrite_primary(None).await?;
    let commit_ts = txn2.commit_primary().await?;
    txn2.commit_secondary(commit_ts).await;

    // As before, but rollback instead of commit.
    let mut txn = client.begin_pessimistic().await?;
    txn.put(vec![1], vec![42]).await?;
    txn.delete(vec![1]).await?;
    txn.delete(vec![2]).await?;
    // Same with upper comment.
    //
    // txn.insert(vec![2], vec![42]).await?;
    txn.delete(vec![2]).await?;
    txn.put(vec![3], vec![42]).await?;
    txn.rollback().await?;

    let mut txn2 = client.begin_optimistic().await?;
    txn2.put(vec![1], vec![42]).await?;
    txn2.put(vec![2], vec![42]).await?;
    txn2.put(vec![3], vec![42]).await?;
    // txn2.commit().await?;
    txn2.prewrite_primary(None).await?;
    let commit_ts = txn2.commit_primary().await?;
    txn2.commit_secondary(commit_ts).await;

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_lock_keys() -> Result<()> {
    init().await?;
    let client = TransactionClient::new_with_config(pd_addrs(), Default::default()).await?;

    let k1 = b"key1".to_vec();
    let k2 = b"key2".to_vec();
    let v = b"some value".to_vec();

    // optimistic
    let mut t1 = client.begin_optimistic().await?;
    let mut t2 = client.begin_optimistic().await?;
    t1.lock_keys(vec![k1.clone(), k2.clone()]).await?;
    t2.put(k1.clone(), v.clone()).await?;
    // t2.commit().await?;
    t2.prewrite_primary(None).await?;
    let commit_ts = t2.commit_primary().await?;
    t2.commit_secondary(commit_ts).await;

    // must have commit conflict
    assert!(t1.commit().await.is_err());

    // pessimistic
    let k3 = b"key3".to_vec();
    let k4 = b"key4".to_vec();
    let mut t3 = client.begin_pessimistic().await?;
    let mut t4 = client.begin_pessimistic().await?;
    t3.lock_keys(vec![k3.clone(), k4.clone()]).await?;
    assert!(t4.lock_keys(vec![k3.clone(), k4.clone()]).await.is_err());

    t3.rollback().await?;
    t4.lock_keys(vec![k3.clone(), k4.clone()]).await?;
    t4.commit().await?;

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_lock_keys_error_handle() -> Result<()> {
    init().await?;
    let client = TransactionClient::new_with_config(pd_addrs(), Default::default()).await?;

    // Keys in `k` should locate in different regions. See `init()` for boundary of regions.
    let k: Vec<Key> = vec![
        0x00000000_u32,
        0x40000000_u32,
        0x80000000_u32,
        0xC0000000_u32,
    ]
    .into_iter()
    .map(|x| x.to_be_bytes().to_vec().into())
    .collect();

    let mut t1 = client.begin_pessimistic().await?;
    let mut t2 = client.begin_pessimistic().await?;
    let mut t3 = client.begin_pessimistic().await?;

    t1.lock_keys(vec![k[0].clone(), k[1].clone()]).await?;
    assert!(t2
        .lock_keys(vec![k[0].clone(), k[2].clone()])
        .await
        .is_err());
    t3.lock_keys(vec![k[2].clone(), k[3].clone()]).await?;

    t1.rollback().await?;
    t3.rollback().await?;

    t2.lock_keys(vec![k[0].clone(), k[2].clone()]).await?;
    // t2.commit().await?;
    t2.prewrite_primary(None).await?;
    let commit_ts = t2.commit_primary().await?;
    t2.commit_secondary(commit_ts).await;

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_get_for_update() -> Result<()> {
    init().await?;
    let client = TransactionClient::new_with_config(pd_addrs(), Default::default()).await?;
    let key1 = "key".to_owned();
    let key2 = "another key".to_owned();
    let value1 = b"some value".to_owned();
    let value2 = b"another value".to_owned();
    let keys = vec![key1.clone(), key2.clone()];

    let mut t1 = client.begin_pessimistic().await?;
    let mut t2 = client.begin_pessimistic().await?;
    let mut t3 = client.begin_optimistic().await?;
    let mut t4 = client.begin_optimistic().await?;
    let mut t0 = client.begin_pessimistic().await?;
    t0.put(key1.clone(), value1).await?;
    t0.put(key2.clone(), value2).await?;
    // t0.commit().await?;
    t0.prewrite_primary(None).await?;
    let commit_ts = t0.commit_primary().await?;
    t0.commit_secondary(commit_ts).await;

    assert!(t1.get(key1.clone()).await?.is_none());
    assert!(t1.get_for_update(key1.clone()).await?.unwrap() == value1);
    // t1.commit().await?;
    t1.prewrite_primary(None).await?;
    let commit_ts = t1.commit_primary().await?;
    t1.commit_secondary(commit_ts).await;

    assert!(t2.batch_get(keys.clone()).await?.count() == 0);
    let res: HashMap<_, _> = t2
        .batch_get_for_update(keys.clone())
        .await?
        .into_iter()
        .map(From::from)
        .collect();
    // t2.commit().await?;
    t2.prewrite_primary(None).await?;
    let commit_ts = t2.commit_primary().await?;
    t2.commit_secondary(commit_ts).await;

    assert!(res.get(&key1.clone().into()).unwrap() == &value1);
    assert!(res.get(&key2.into()).unwrap() == &value2);

    assert!(t3.get_for_update(key1).await?.is_none());
    assert!(t3.commit().await.is_err());

    assert!(t4.batch_get_for_update(keys).await?.is_empty());
    assert!(t4.commit().await.is_err());

    Ok(())
}

#[tokio::test]
#[serial]
async fn txn_pessimistic_heartbeat() -> Result<()> {
    init().await?;

    let key1 = "key1".to_owned();
    let key2 = "key2".to_owned();
    let client = TransactionClient::new(pd_addrs()).await?;

    let mut heartbeat_txn = client
        .begin_with_options(TransactionOptions::new_pessimistic())
        .await?;
    heartbeat_txn.put(key1.clone(), "foo").await.unwrap();

    let mut txn_without_heartbeat = client
        .begin_with_options(
            TransactionOptions::new_pessimistic().heartbeat_option(HeartbeatOption::NoHeartbeat),
        )
        .await?;
    txn_without_heartbeat
        .put(key2.clone(), "fooo")
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(23)).await;

    // use other txns to check these locks
    let mut t3 = client
        .begin_with_options(TransactionOptions::new_optimistic().no_resolve_locks())
        .await?;
    t3.put(key1.clone(), "gee").await?;
    assert!(t3.commit().await.is_err());
    let mut t4 = client.begin_optimistic().await?;
    t4.put(key2.clone(), "geee").await?;
    // t4.commit().await?;
    t4.prewrite_primary(None).await?;
    let commit_ts = t4.commit_primary().await?;
    t4.commit_secondary(commit_ts).await;

    assert!(heartbeat_txn.commit().await.is_ok());
    assert!(txn_without_heartbeat.commit().await.is_err());

    Ok(())
}

#[tokio::test]
#[serial]
async fn tianhe_txn_commit() -> Result<()> {
    init().await?;
    // let file = OpenOptions::new()
    //     .create(true)
    //     .write(true)
    //     .truncate(false)
    //     .open("test.log")
    //     .expect("open log file failed");

    // let decorator = slog_term::PlainDecorator::new(file);
    // let drain = slog_term::FullFormat::new(decorator)
    //     .build()
    //     .filter_level(slog::Level::Debug)
    //     .fuse();
    // let drain = slog_async::Async::new(drain).build().fuse();
    // let log = slog::Logger::root(drain, o!());
    let client = TransactionClient::new_with_config(pd_addrs(), Default::default()).await?;
    let key1 = "key".to_owned();
    let key2 = "another key".to_owned();
    let value1 = b"some value".to_owned();
    let value2 = b"another value".to_owned();

    let mut t0 = client.begin_optimistic().await?;
    t0.put(key1.clone(), value1).await?;
    t0.put(key2.clone(), value2).await?;
    let a = "a".to_owned();
    let b = "b".to_owned();
    let c = "c".to_owned();
    let d = "d".to_owned();
    let e = "e".to_owned();
    let f = "f".to_owned();
    t0.put(a.clone(), a.clone()).await?;
    t0.put(b.clone(), b.clone()).await?;
    t0.put(c.clone(), c.clone()).await?;

    let mut t1 = client.begin_optimistic().await?;
    t1.put("d".to_owned(), "d".to_owned()).await?;
    t1.put("e".to_owned(), "e".to_owned()).await?;
    t1.put("f".to_owned(), "f".to_owned()).await?;

    // t0.commit().await?;
    let (primary_key, start_ts) = t0.prewrite_primary(None).await?;
    t1.prewrite_secondary(primary_key, start_ts).await?;

    let commit_ts = t0.commit_primary().await?;
    t0.commit_secondary(commit_ts.clone()).await;
    t1.commit_secondary(commit_ts).await;

    // test snapshot batch get
    let mut snapshot = client.snapshot(
        client.current_timestamp().await?,
        TransactionOptions::new_optimistic(),
    );
    let batch_get_res: HashMap<Key, Value> = snapshot
        .batch_get(vec![
            a.clone(),
            b.clone(),
            c.clone(),
            d.clone(),
            e.clone(),
            f.clone(),
        ])
        .await?
        .map(|pair| (pair.0, pair.1))
        .collect();
    assert!(batch_get_res.get(&Key::from(a.clone())).unwrap() == a.as_bytes());
    assert!(batch_get_res.get(&Key::from(b.clone())).unwrap() == b.as_bytes());
    assert!(batch_get_res.get(&Key::from(c.clone())).unwrap() == c.as_bytes());
    assert!(batch_get_res.get(&Key::from(d.clone())).unwrap() == d.as_bytes());
    assert!(batch_get_res.get(&Key::from(e.clone())).unwrap() == e.as_bytes());
    assert!(batch_get_res.get(&Key::from(f.clone())).unwrap() == f.as_bytes());
    Ok(())
}

#[tokio::test]
#[serial]
async fn tianhe_txn_prewrite_rollback() -> Result<()> {
    init().await?;
    let client = TransactionClient::new_with_config(pd_addrs(), Default::default()).await?;
    let key1 = "key".to_owned();
    let key2 = "another key".to_owned();
    let value1 = b"some value".to_owned();
    let value2 = b"another value".to_owned();

    let a = "a".to_owned();
    let b = "b".to_owned();
    let c = "c".to_owned();
    let d = "d".to_owned();
    let e = "e".to_owned();
    let f = "f".to_owned();
    let mut t0 = client.begin_optimistic().await?;
    t0.put(key1.clone(), value1).await?;
    t0.put(key2.clone(), value2).await?;
    t0.put(a.clone(), a.clone()).await?;
    t0.put(b.clone(), b.clone()).await?;
    t0.put(c.clone(), c.clone()).await?;
    t0.commit().await?;

    let mut t0 = client.begin_optimistic().await?;
    t0.put(a.clone(), "a1".to_owned()).await?;
    t0.put(b.clone(), "b1".to_owned()).await?;
    t0.put(c.clone(), "c1".to_owned()).await?;

    let mut t1 = client.begin_optimistic().await?;
    t1.put("d".to_owned(), "d".to_owned()).await?;
    t1.put("e".to_owned(), "e".to_owned()).await?;
    t1.put("f".to_owned(), "f".to_owned()).await?;

    // t0.commit().await?;
    let (primary_key, start_ts) = t0.prewrite_primary(None).await?;
    t1.prewrite_secondary(primary_key, start_ts).await?;

    t0.rollback().await?;
    t1.rollback().await?;
    // let commit_ts = t0.commit_primary().await?;
    // t0.commit_secondary(commit_ts.clone()).await;
    // t1.commit_secondary(commit_ts).await;

    // test snapshot batch get
    let mut snapshot = client.snapshot(
        client.current_timestamp().await?,
        TransactionOptions::new_optimistic(),
    );
    let batch_get_res: HashMap<Key, Value> = snapshot
        .batch_get(vec![
            a.clone(),
            b.clone(),
            c.clone(),
            d.clone(),
            e.clone(),
            f.clone(),
        ])
        .await?
        .map(|pair| (pair.0, pair.1))
        .collect();
    assert!(batch_get_res.get(&Key::from(a.clone())).unwrap() == a.as_bytes());
    assert!(batch_get_res.get(&Key::from(b.clone())).unwrap() == b.as_bytes());
    assert!(batch_get_res.get(&Key::from(c.clone())).unwrap() == c.as_bytes());
    assert!(batch_get_res.get(&Key::from(d.clone())).is_none());
    assert!(batch_get_res.get(&Key::from(e.clone())).is_none());
    assert!(batch_get_res.get(&Key::from(f.clone())).is_none());
    Ok(())
}

#[tokio::test]
#[serial]
async fn tianhe_txn_rollback_after_primary_committed() -> Result<()> {
    init().await?;
    let client = TransactionClient::new_with_config(pd_addrs(), Default::default()).await?;
    let loop_count = 10;
    let mut t0 = client.begin_optimistic().await?;
    let a = "a".to_owned();
    let b = "b".to_owned();
    let c = "c".to_owned();
    t0.put(a.clone(), a.clone()).await?;
    t0.put(b.clone(), b.clone()).await?;
    t0.put(c.clone(), c.clone()).await?;

    let mut t1 = client.begin_optimistic().await?;
    let mut keys = vec![];
    let mut values = vec![];
    for j in 0..loop_count {
        let key = format!("key_________________________{}", j);
        let value: Vec<u8> = (0..1024).map(|_| rand::random::<u8>()).collect();
        keys.push(key.clone());
        values.push(value.clone());
        t1.put(key.clone(), value.clone()).await?;
    }
    println!("start prewrite");
    let start = Instant::now();
    let (primary_key, start_ts) = t0.prewrite_primary(None).await?;
    t1.prewrite_secondary(primary_key, start_ts).await?;
    let prewrite_duration = start.elapsed();
    let start = Instant::now();
    let _commit_ts = t0.commit_primary().await?;
    t0.rollback().await?;
    t1.rollback().await?;
    // t1.commit_secondary(commit_ts).await;
    let commit_duration = start.elapsed();
    println!(
        "Time elapsed in prewrite is: {:?},in commit is: {:?}",
        prewrite_duration, commit_duration
    );
    // check
    let mut snapshot = client.snapshot(
        client.current_timestamp().await?,
        TransactionOptions::new_optimistic(),
    );
    let batch_get_res: HashMap<Key, Value> = snapshot
        .batch_get(keys.clone())
        .await?
        .map(|pair| (pair.0, pair.1))
        .collect();
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(
            batch_get_res.get(&Key::from(k.clone())).unwrap(),
            values.get(i).unwrap()
        );
    }
    Ok(())
}

#[tokio::test]
#[serial]
async fn tianhe_txn_resolve_locks_before_expired() -> Result<()> {
    init().await?;
    let client = TransactionClient::new_with_config(pd_addrs(), Default::default()).await?;
    let loop_count = 10;
    let mut t0 = client.begin_optimistic().await?;
    let a = "a".to_owned();
    let b = "b".to_owned();
    let c = "c".to_owned();
    t0.put(a.clone(), a.clone()).await?;
    t0.put(b.clone(), b.clone()).await?;
    t0.put(c.clone(), c.clone()).await?;

    let mut t1 = client.begin_optimistic().await?;
    let mut keys = vec![];
    let mut values = vec![];
    for j in 0..loop_count {
        let key = format!("key_________________________{}", j);
        let value: Vec<u8> = (0..1024).map(|_| rand::random::<u8>()).collect();
        keys.push(key.clone());
        values.push(value.clone());
        t1.put(key.clone(), value.clone()).await?;
    }
    println!("start prewrite");
    let start = Instant::now();
    let (primary_key, start_ts) = t0.prewrite_primary(None).await?;
    t1.prewrite_secondary(primary_key, start_ts).await?;
    let prewrite_duration = start.elapsed();
    let start = Instant::now();
    let commit_ts = t0.commit_primary().await?;
    t0.commit_secondary(commit_ts.clone()).await;
    // t1.commit_secondary(commit_ts).await;
    let commit_duration = start.elapsed();
    println!(
        "Time elapsed in prewrite is: {:?},in commit is: {:?}",
        prewrite_duration, commit_duration
    );
    // check
    let mut snapshot = client.snapshot(
        client.current_timestamp().await?,
        TransactionOptions::new_optimistic(),
    );
    let batch_get_res: HashMap<Key, Value> = snapshot
        .batch_get(keys.clone())
        .await?
        .map(|pair| (pair.0, pair.1))
        .collect();
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(
            batch_get_res.get(&Key::from(k.clone())).unwrap(),
            values.get(i).unwrap()
        );
    }
    Ok(())
}

#[tokio::test]
#[serial]
async fn tianhe_txn_commit100() -> Result<()> {
    init().await?;
    let client = TransactionClient::new_with_config(pd_addrs(), Default::default()).await?;
    // FIXME: if the commit size of a region is large than raft-entry-max-size maybe failed
    let commit_size = 1024;
    let value_size = 512;
    let loop_count = 20;
    for i in 0..loop_count {
        let mut t0 = client.begin_optimistic().await?;
        let mut t1 = client.begin_optimistic().await?;
        let mut keys = vec![];
        let mut values = vec![];
        let mut keys2 = vec![];
        let mut values2 = vec![];
        for _j in 0..commit_size {
            let key: Vec<u8> = b"s_txs_"
                .to_vec()
                .into_iter()
                .chain((0..40).map(|_| rand::random::<u8>()))
                .collect();
            let value: Vec<u8> = (0..value_size).map(|_| rand::random::<u8>()).collect();
            keys.push(key.clone());
            values.push(value.clone());
            t0.put(key.clone(), value.clone()).await?;
        }
        for _j in 0..commit_size * 5 {
            let key: Vec<u8> = b"s_receipts_"
                .to_vec()
                .into_iter()
                .chain((0..32).map(|_| rand::random::<u8>()))
                .collect();
            let value: Vec<u8> = (0..value_size).map(|_| rand::random::<u8>()).collect();
            keys2.push(key.clone());
            values2.push(value.clone());
            t1.put(key.clone(), value.clone()).await?;
        }
        println!("{} start prewrite", i);
        let start = Instant::now();
        let (primary_key, start_ts) = t0.prewrite_primary(None).await?;
        let primary_prewrite_duration = start.elapsed();
        t1.prewrite_secondary(primary_key, start_ts).await?;
        let prewrite_duration = start.elapsed();
        let start = Instant::now();
        let commit_ts = t0.commit_primary().await?;
        t0.commit_secondary(commit_ts.clone()).await;
        let primary_commit_duration = start.elapsed();
        t1.commit_secondary(commit_ts).await;
        let commit_duration = start.elapsed();
        println!(
            "{} Time elapsed in prewrite is: {:?}/{:?},in commit is: {:?}/{:?}",
            i,
            primary_prewrite_duration,
            prewrite_duration,
            primary_commit_duration,
            commit_duration
        );
        // check
        let mut snapshot = client.snapshot(
            client.current_timestamp().await?,
            TransactionOptions::new_optimistic(),
        );
        let batch_get_res: HashMap<Key, Value> = snapshot
            .batch_get(keys.clone())
            .await?
            .map(|pair| (pair.0, pair.1))
            .collect();
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(
                batch_get_res.get(&Key::from(k.clone())).unwrap(),
                values.get(i).unwrap()
            );
        }
    }
    Ok(())
}

// helper function
async fn get_txn_u32(txn: &mut Transaction, key: Vec<u8>) -> Result<u32> {
    let x = txn.get(key).await?.unwrap();
    let boxed_slice = x.into_boxed_slice();
    let array: Box<[u8; 4]> = boxed_slice
        .try_into()
        .expect("Value should not exceed u32 (4 * u8)");
    Ok(u32::from_be_bytes(*array))
}

// helper function
fn gen_u32_keys(num: u32, rng: &mut impl Rng) -> HashSet<Vec<u8>> {
    let mut set = HashSet::new();
    for _ in 0..num {
        set.insert(rng.gen::<u32>().to_be_bytes().to_vec());
    }
    set
}
