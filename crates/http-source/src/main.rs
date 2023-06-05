mod config;
mod formatter;
mod record_stream;
mod source;
mod streaming_response_formatter;

use anyhow::Result;
use async_std::stream::StreamExt;
use config::HttpConfig;
use fluvio::{RecordKey, TopicProducer};
use fluvio_connector_common::{
    connector,
    tracing::{debug, trace},
    Source,
};

use crate::{
    source::HttpSource,
    streaming_response_formatter::StreamingResponseFormatter,
};

#[connector(source)]
async fn start(config: HttpConfig, producer: TopicProducer) -> Result<()> {
    debug!(?config);

    let source = HttpSource::new(&config)?;
    let http_response = source.http_response().await?;

    match response_headers(&http_response)?.contains("chunked") {
        false => {
            debug!("Polling endpoint");

            let item =
                source.formatter.response_to_string(http_response).await?;
            producer.send(RecordKey::NULL, item).await?;

            let mut stream = source.connect(None).await?;
            while let Some(item) = stream.next().await {
                trace!(?item);
                producer.send(RecordKey::NULL, item).await?;
            }
        }
        true => {
            debug!("Streaming from endpoint");

            let streaming_response_formatter = StreamingResponseFormatter::new(
                source.formatter.clone(),
                http_response,
            )?;

            let http_response_chunk_stream =
                source.http_response_stream().await?;
            source
                .produce_streaming_data(
                    http_response_chunk_stream,
                    streaming_response_formatter,
                    producer,
                )
                .await?;
        }
    }

    Ok(())
}

fn response_headers(response: &reqwest::Response) -> Result<String> {
    let mut headers = String::new();
    for (key, value) in response.headers() {
        headers.push_str(&format!("{}: {}\n", key, value.to_str()?));
    }
    Ok(headers)
}
