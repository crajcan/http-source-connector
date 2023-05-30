mod config;
mod formatter;
mod http_response_stream;
mod source;

use anyhow::Result;
use config::HttpConfig;
use fluvio::TopicProducer;
use fluvio_connector_common::{connector, tracing::debug};

use crate::source::HttpSource;

#[connector(source)]
async fn start(config: HttpConfig, producer: TopicProducer) -> Result<()> {
    debug!(?config);

    let source = HttpSource::new(&config)?;

    // let mut stream = source.connect(None).await?;
    // while let Some(item) = stream.next().await {
    //     trace!(?item);
    //     producer.send(RecordKey::NULL, item).await?;
    // }

    let http_response_stream = source.http_response_stream().await?;
    source
        .produce_streaming_data(http_response_stream, producer)
        .await?;

    Ok(())
}
