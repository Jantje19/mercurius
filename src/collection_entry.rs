use std::sync::Arc;

use mongodb::{
    bson::Document,
    change_stream::{
        event::{ChangeStreamEvent, OperationType},
        ChangeStream,
    },
    options::{ChangeStreamOptions, FullDocumentBeforeChangeType, FullDocumentType},
    Collection,
};
use tokio::{
    sync::{mpsc::UnboundedSender, Mutex},
    task::{AbortHandle, JoinSet},
};

use crate::subscription::{Event, Subscription};

use self::subscriptions_manager::{
    SubscriptionHandle, SubscriptionsManager, SubscriptionsManagerError,
};

pub mod subscriptions_manager {
    use std::{collections::HashMap, fmt::Display};

    use crate::subscription::Subscription;

    #[derive(Debug, Hash, PartialEq, Eq, Clone, PartialOrd, Ord)]
    pub struct SubscriptionHandle(usize);

    #[derive(Debug)]
    pub enum SubscriptionsManagerError {
        NoFreeSlot,
    }

    #[derive(Debug)]
    pub(crate) struct SubscriptionsManager {
        subscriptions: HashMap<SubscriptionHandle, Subscription>,
        has_been_filled: bool,
        next_index: usize,
    }

    impl SubscriptionsManager {
        pub fn new() -> Self {
            Self {
                subscriptions: HashMap::new(),
                has_been_filled: false,
                next_index: 0,
            }
        }

        pub(crate) fn len(&self) -> usize {
            self.subscriptions.len()
        }

        pub(crate) fn get_all(&self) -> impl Iterator<Item = &Subscription> {
            self.subscriptions.values()
        }

        pub(crate) fn add(
            &mut self,
            subscription: Subscription,
        ) -> Result<SubscriptionHandle, SubscriptionsManagerError> {
            let handle = SubscriptionHandle(self.next_index);
            let new_index = if self.has_been_filled {
                self.find_free_index()?
            } else {
                match self.next_index.checked_add(1) {
                    Some(v) => v,
                    None => {
                        self.has_been_filled = true;
                        self.find_free_index()?
                    }
                }
            };

            self.subscriptions.insert(handle.clone(), subscription);
            self.next_index = new_index;
            Ok(handle)
        }

        pub(crate) fn remove(&mut self, handle: SubscriptionHandle) {
            self.subscriptions.remove(&handle);
        }

        fn find_free_index(&self) -> Result<usize, SubscriptionsManagerError> {
            let mut keys: Vec<_> = self.subscriptions.keys().collect();
            keys.sort();

            #[allow(clippy::needless_range_loop)]
            for i in 0..std::cmp::min(keys.len(), usize::MAX) {
                if keys[i].0 != i {
                    return Ok(i);
                }
            }

            Err(SubscriptionsManagerError::NoFreeSlot)
        }
    }

    impl Display for SubscriptionsManagerError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(match self {
                SubscriptionsManagerError::NoFreeSlot => "No free slot",
            })
        }
    }

    impl std::error::Error for SubscriptionsManagerError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            None
        }

        fn description(&self) -> &str {
            "description() is deprecated; use Display"
        }

        fn cause(&self) -> Option<&dyn std::error::Error> {
            self.source()
        }
    }
}

#[derive(Debug)]
pub struct CollectionEntry {
    // TODO: Convert to RwLock?
    subscriptions: Arc<Mutex<SubscriptionsManager>>,
    change_stream_handle: AbortHandle,
}

impl CollectionEntry {
    pub async fn new(
        collection: Collection<Document>,
        join_set: &mut JoinSet<()>,
    ) -> Result<Self, mongodb::error::Error> {
        let change_stream = collection
            .watch(
                None,
                ChangeStreamOptions::builder()
                    .full_document(Some(FullDocumentType::UpdateLookup))
                    .full_document_before_change(Some(FullDocumentBeforeChangeType::WhenAvailable))
                    .build(),
            )
            .await?;

        let subscriptions = Arc::new(Mutex::new(SubscriptionsManager::new()));

        let event_subscriptions = subscriptions.clone();
        let change_stream_handle = join_set.spawn(async move {
            // TODO: Remove `unwrap`
            CollectionEntry::handle_events(event_subscriptions, change_stream)
                .await
                .unwrap();
        });

        Ok(Self {
            subscriptions,
            change_stream_handle,
        })
    }

    pub async fn add_subscription(
        &self,
        filter: impl Into<Option<Document>>,
        channel: UnboundedSender<Event>,
    ) -> Result<SubscriptionHandle, SubscriptionsManagerError> {
        let filter = filter.into();

        self.subscriptions
            .lock()
            .await
            .add(Subscription::new(filter, channel))
    }

    pub async fn remove_subscription(&self, handle: SubscriptionHandle) {
        self.subscriptions.lock().await.remove(handle);
    }

    pub async fn subscription_count(&self) -> usize {
        self.subscriptions.lock().await.len()
    }

    async fn handle_events(
        subscriptions: Arc<Mutex<SubscriptionsManager>>,
        mut change_stream: ChangeStream<ChangeStreamEvent<Document>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        fn get_key(document_key: Option<Document>) -> String {
            // TODO: Do we want to unwrap here?
            document_key
                .unwrap()
                .get("_id")
                .unwrap()
                .as_str()
                .unwrap()
                .to_string()
        }

        // TODO: Use resume tokens
        // let mut resume_token = None;
        // TODO: Don't unwrap here
        // TODO: Keep looping over the subscriptions when a send fails
        while change_stream.is_alive() {
            if let Some(event) = change_stream.next_if_any().await? {
                // TODO: Use rayon
                match event.operation_type {
                    OperationType::Insert => {
                        let doc = event
                            .full_document
                            .expect("the inserted document should be available");
                        let subscriptions = subscriptions.lock().await;

                        let doc = Arc::new(doc);

                        for subscription in subscriptions.get_all() {
                            subscription.handle_insert(&doc)?;
                        }
                    }
                    OperationType::Delete => {
                        let key = get_key(event.document_key);
                        let subscriptions = subscriptions.lock().await;

                        let doc = event
                            .full_document_before_change
                            .expect("the deleted document should be available");
                        let key = Arc::new(key.to_string());

                        for subscription in subscriptions.get_all() {
                            subscription.handle_delete(&key, &doc)?;
                        }
                    }
                    OperationType::Update => {
                        let key = get_key(event.document_key);
                        let subscriptions = subscriptions.lock().await;

                        let update = Arc::new(
                            event
                                .update_description
                                .expect("the updated values should be available"),
                        );
                        let new_doc = Arc::new(
                            event
                                .full_document
                                .expect("the new document should be available for this update"),
                        );
                        let old_doc = event
                            .full_document_before_change
                            .expect("the old document should be available for this update");
                        let key = Arc::new(key.to_string());

                        for subscription in subscriptions.get_all() {
                            subscription.handle_update(&key, &update, &old_doc, &new_doc)?;
                        }
                    }
                    OperationType::Replace => {
                        let key = get_key(event.document_key);
                        let subscriptions = subscriptions.lock().await;

                        let new_doc =
                            Arc::new(event.full_document.expect(
                                "the new document should be available for this replacement",
                            ));
                        let old_doc = event
                            .full_document_before_change
                            .expect("the old document should be available for this replacement");
                        let key = Arc::new(key.to_string());

                        for subscription in subscriptions.get_all() {
                            subscription.handle_replace(&key, &old_doc, &new_doc)?;
                        }
                    }
                    OperationType::DropDatabase
                    | OperationType::Drop
                    | OperationType::Rename
                    | OperationType::Invalidate => {
                        let subscriptions = subscriptions.lock().await;

                        for subscription in subscriptions.get_all() {
                            subscription.handle_drop()?;
                        }
                    }
                    // TODO: Don't panic?
                    OperationType::Other(event) => panic!(
                        "Received a change event that we don't know how to handle: {}",
                        event
                    ),
                    _ => panic!("Operation type {:?} not implemented", event.operation_type),
                }
            }

            // resume_token = change_stream.resume_token();
        }

        Err(Box::new(mongodb::error::Error::custom(
            "change stream ended",
        )))
    }
}

impl Drop for CollectionEntry {
    fn drop(&mut self) {
        // `AbortHandle` does implement `Drop`, but just to be extra safe
        self.change_stream_handle.abort()
    }
}
