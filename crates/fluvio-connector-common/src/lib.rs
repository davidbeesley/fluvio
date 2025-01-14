pub mod producer;
pub mod smartmodule;
pub mod monitoring;

#[cfg(feature = "derive")]
pub use fluvio_connector_derive::connector;

pub use fluvio_connector_package::config::ConnectorConfig;

use fluvio::{Offset, metadata::topic::TopicSpec};
use futures::stream::LocalBoxStream;
use async_trait::async_trait;

pub type Error = anyhow::Error;
pub type Result<T> = std::result::Result<T, Error>;

pub mod future {
    pub use fluvio_future::task::run_block_on;
    pub use fluvio_future::subscriber::init_logger;
}

pub mod tracing {
    pub use ::tracing::*;
}

#[async_trait]
pub trait Source<'a, I> {
    async fn connect(self, offset: Option<Offset>) -> Result<LocalBoxStream<'a, I>>;
}

pub async fn ensure_topic_exists(config: &ConnectorConfig) -> Result<()> {
    let admin = fluvio::FluvioAdmin::connect().await?;
    let topics = admin
        .list::<TopicSpec, String>(vec![config.topic.clone()])
        .await?;
    let topic_exists = topics.iter().any(|t| t.name.eq(&config.topic));
    if !topic_exists {
        let _ = admin
            .create(
                config.topic.clone(),
                false,
                TopicSpec::new_computed(1, 1, Some(false)),
            )
            .await;
    }
    Ok(())
}
