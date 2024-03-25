use std::{sync::Arc, time::Duration};

use mercurius::Mercurius;
use mongodb::{bson::doc, options::ClientOptions, Client};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client_options = ClientOptions::parse("mongodb://localhost:27017").await?;
    client_options.connect_timeout = Some(Duration::from_secs(1));
    client_options.app_name = Some("Mercurius".to_string());
    let client = Client::with_options(client_options)?;
    let db = client.database("mrw");

    let mercurius = Arc::new(Mercurius::new(db));

    let (mut receiver, handle) = mercurius
        .add("test", doc! { "name": "test" })
        .await
        .unwrap();

    tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            println!("Event: {:?}", event);
        }
    });

    println!("Running...");

    {
        let mercurius = mercurius.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            mercurius.remove(handle).await;
        });
    }

    mercurius.run().await?;

    Ok(())
}
