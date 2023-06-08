mod config;
mod formatter;
mod polling_source;
mod record_stream;
mod source;
mod streaming_source;

use anyhow::Result;
use async_std::stream::StreamExt;
use config::HttpConfig;
use fluvio::{RecordKey, TopicProducer};
use fluvio_connector_common::{
    connector,
    tracing::{debug, error, trace},
    Source,
};
use polling_source::PollingSource;
use streaming_source::StreamingSource;

use crate::source::HttpSource;

#[connector(source)]
async fn start(config: HttpConfig, producer: TopicProducer) -> Result<()> {
    debug!(?config);

    let source = HttpSource::new(&config)?;
    let initial_response = source.issue_request().await?;

    match response_headers(&initial_response)?.contains("chunked") {
        false => {
            debug!("Polling endpoint");

            // produce record from initial request
            let first_record = source
                .formatter
                .response_to_string(initial_response)
                .await?;
            producer.send(RecordKey::NULL, first_record).await?;

            let polling_source = PollingSource::new(source);
            let mut polling_stream = polling_source.connect(None).await?;
            while let Some(item) = polling_stream.next().await {
                trace!(?item);
                producer.send(RecordKey::NULL, item).await?;
            }
        }
        true => {
            debug!("Streaming from endpoint");

            let streaming_source =
                StreamingSource::new(source, initial_response)?;
            let mut continuous_stream = streaming_source.connect(None).await?;

            while let Some(chunk) = continuous_stream.next().await {
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(err) => {
                        error!("Chunk retrieval failed: {}", err);
                        continue;
                    }
                };

                producer.send(RecordKey::NULL, chunk).await?;
            }
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
