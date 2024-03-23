use std::sync::Arc;

use mongodb::{bson::Document, change_stream::event::UpdateDescription};
use serde_json_matcher::{from_json, ObjMatcher};
use tokio::sync::mpsc::{error::SendError, UnboundedSender};

#[derive(Debug)]
pub enum Event {
    Added(Arc<Document>),
    Removed(Arc<String>),
    Updated((Arc<String>, Arc<UpdateDescription>)),
    Replaced((Arc<String>, Arc<Document>)),
    /// Something happend that requires the subscription to be removed.
    /// This can occur when the collection or database has been dropped or the collection has been renamed or the stream was invalidated.
    Drop,
}

// TODO: Share subscription matcher across multiple channels
#[derive(Debug)]
pub struct Subscription {
    selector: Option<ObjMatcher>,
    channel: UnboundedSender<Event>,
}

impl Subscription {
    pub fn new(selector: Option<Document>, channel: UnboundedSender<Event>) -> Self {
        let selector = selector
            .map(|e| from_json(Subscription::document_to_value(&e)).expect("is correct matcher"));

        Self { selector, channel }
    }

    pub fn handle_insert(&self, document: &Arc<Document>) -> Result<(), SendError<Event>> {
        if !self.matches(document) {
            return Ok(());
        };

        self.channel.send(Event::Added(document.clone()))?;
        Ok(())
    }

    pub fn handle_delete(
        &self,
        key: &Arc<String>,
        document: &Document,
    ) -> Result<(), SendError<Event>> {
        if !self.matches(document) {
            return Ok(());
        };

        self.channel.send(Event::Removed(key.clone()))?;

        Ok(())
    }

    pub fn handle_update(
        &self,
        key: &Arc<String>,
        update: &Arc<UpdateDescription>,
        old_doc: &Document,
        new_doc: &Arc<Document>,
    ) -> Result<(), SendError<Event>> {
        let old_doc_matches = self.matches(old_doc);
        let new_doc_matches = self.matches(new_doc);

        // If both documents match then just send the update along
        if old_doc_matches && new_doc_matches {
            self.channel
                .send(Event::Updated((key.clone(), update.clone())))?;
        // If only the old doc matches that means that, as far as the selector is concerned, it has been removed
        } else if old_doc_matches {
            self.channel.send(Event::Removed(key.clone()))?;
        // If only the new doc matches that means that, as far as the selector is concerned, it has been added
        } else if new_doc_matches {
            self.channel.send(Event::Added(new_doc.clone()))?;
        }
        // If neither match, just skip

        Ok(())
    }

    pub fn handle_replace(
        &self,
        key: &Arc<String>,
        old_doc: &Document,
        new_doc: &Arc<Document>,
    ) -> Result<(), SendError<Event>> {
        let old_doc_matches = self.matches(old_doc);
        let new_doc_matches = self.matches(new_doc);

        // If both documents match then just send the replacement along
        if old_doc_matches && new_doc_matches {
            self.channel
                .send(Event::Replaced((key.clone(), new_doc.clone())))?;
        // If only the old doc matches that means that, as far as the selector is concerned, it has been removed
        } else if old_doc_matches {
            self.channel.send(Event::Removed(key.clone()))?;
        // If only the new doc matches that means that, as far as the selector is concerned, it has been added
        } else if new_doc_matches {
            self.channel.send(Event::Added(new_doc.clone()))?;
        }
        // If neither match, just skip

        Ok(())
    }

    pub fn handle_drop(&self) -> Result<(), SendError<Event>> {
        self.channel.send(Event::Drop)
    }

    fn matches(&self, document: &Document) -> bool {
        // https://docs.rs/serde_json_matcher/0.1.5/serde_json_matcher/enum.ObjMatcher.html
        if let Some(matcher) = &self.selector {
            if !matcher.matches(&Subscription::document_to_value(document)) {
                return false;
            }
        }

        true
    }

    fn document_to_value(document: &Document) -> serde_json::Value {
        // TODO: Optimize by doing a direct conversion
        serde_json::from_str(&document.to_string()).unwrap()
    }
}
