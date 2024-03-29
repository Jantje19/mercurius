use std::collections::HashMap;

use collection_entry::{subscriptions_manager::SubscriptionHandle, CollectionEntry};
use mongodb::{
    bson::{doc, Document},
    Database,
};
use subscription::Event;
use tokio::{
    sync::{
        mpsc::{self, UnboundedReceiver},
        Mutex,
    },
    task::JoinSet,
};

mod collection_entry;
pub mod subscription;

pub struct Handle {
    collection_name: String,
    subscription_handle: SubscriptionHandle,
}

pub struct Mercurius {
    collections: Mutex<HashMap<String, CollectionEntry>>,
    join_set: Mutex<JoinSet<()>>,
    db: Database,
}

impl Mercurius {
    pub fn new(db: Database) -> Self {
        Self {
            collections: Mutex::new(HashMap::new()),
            join_set: Mutex::new(JoinSet::new()),
            db,
        }
    }

    pub async fn add(
        &self,
        name: String,
        filter: impl Into<Option<Document>>,
    ) -> Result<(UnboundedReceiver<Event>, Handle), Box<dyn std::error::Error>> {
        self.db
            .run_command(
                doc! { "collMod": name.clone(), "changeStreamPreAndPostImages": { "enabled": true } },
                None,
            )
            .await?;

        let entry = {
            let mut join_set = self.join_set.lock().await;

            CollectionEntry::new(self.db.collection::<Document>(&name), &mut join_set).await?
        };

        let (sender, receiver) = mpsc::unbounded_channel();

        let handle = entry.add_subscription(filter, sender).await?;

        {
            let mut collections = self.collections.lock().await;
            collections.insert(name.clone(), entry);
        }

        Ok((
            receiver,
            Handle {
                collection_name: name.clone(),
                subscription_handle: handle,
            },
        ))
    }

    pub async fn remove(&self, handle: Handle) {
        let mut collections = self.collections.lock().await;

        let collection = match collections.get(&handle.collection_name) {
            Some(collection) => collection,
            None => return,
        };

        collection
            .remove_subscription(handle.subscription_handle)
            .await;

        if collection.subscription_count().await == 0 {
            collections.remove(&handle.collection_name);
        }
    }

    pub async fn run(&self) -> Result<(), Box<tokio::task::JoinError>> {
        let mut join_set = self.join_set.lock().await;

        while let Some(res) = join_set.join_next().await {
            if let Err(e) = res {
                if e.is_panic() {
                    Err(Box::new(e))?;
                }
            }
        }

        Ok(())
    }
}
