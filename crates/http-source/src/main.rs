mod config;
mod formatter;
mod record_stream;
mod source;
mod streaming_response_formatter;

use anyhow::Result;
use config::HttpConfig;
use fluvio::TopicProducer;
use fluvio_connector_common::{connector, tracing::debug};

use crate::{
    source::HttpSource,
    streaming_response_formatter::StreamingResponseFormatter,
};

#[connector(source)]
async fn start(config: HttpConfig, producer: TopicProducer) -> Result<()> {
    debug!(?config);

    let source = HttpSource::new(&config)?;

    // let mut stream = source.connect(None).await?;
    // while let Some(item) = stream.next().await {
    //     trace!(?item);
    //     producer.send(RecordKey::NULL, item).await?;
    // }

    let http_response = source.http_response().await?;
    let streaming_response_formatter = StreamingResponseFormatter::new(
        source.formatter.clone(),
        http_response,
    )?;
    let http_response_chunk_stream = source.http_response_stream().await?;
    source
        .produce_streaming_data(
            http_response_chunk_stream,
            streaming_response_formatter,
            producer,
        )
        .await?;

    Ok(())
}
