use std::collections::HashMap;

use collection_entry::CollectionEntry;
use mongodb::{
    bson::{doc, Document},
    options::ClientOptions,
    Client, Database,
};
use tokio::{sync::mpsc, task::JoinSet};

mod collection_entry;
mod subscription;

async fn add_collection_entry<'a>(
    name: &'a str,
    db: &Database,
    collections: &mut HashMap<&'a str, CollectionEntry>,
    join_set: &mut JoinSet<()>,
) -> Result<(), mongodb::error::Error> {
    db.run_command(
        doc! { "collMod": name, "changeStreamPreAndPostImages": { "enabled": true } },
        None,
    )
    .await?;

    let entry = CollectionEntry::new(db.collection::<Document>(name), join_set).await?;

    collections.insert(name, entry);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client_options = ClientOptions::parse("mongodb://localhost:27017").await?;
    client_options.app_name = Some("Mercurius".to_string());
    let client = Client::with_options(client_options)?;
    let db = client.database("mrw");

    let mut collections = HashMap::new();
    let mut join_set = JoinSet::new();

    add_collection_entry("test", &db, &mut collections, &mut join_set).await?;

    let (sender, mut receiver) = mpsc::unbounded_channel();

    let handle = collections
        .get("test")
        .unwrap()
        // .add_subscription(doc! { "name": "test" }, sender)
        .add_subscription(doc! {}, sender)
        .await
        .unwrap();

    tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            println!("Event: {:?}", event);
        }
    });

    println!("Running...");

    // {
    //     let name = "test";
    //     let collection = collections.get(name).unwrap();
    //     collection.remove_subscription(handle).await;

    //     if collection.subscription_count().await == 0 {
    //         collections.remove(name);
    //     }
    // }

    while let Some(res) = join_set.join_next().await {
        if let Err(e) = res {
            if e.is_panic() {
                Err(Box::new(e))?;
            }
        }
    }

    Ok(())
}
