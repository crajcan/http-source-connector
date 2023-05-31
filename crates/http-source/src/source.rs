use crate::record_stream::*;
use crate::{
    config::HttpConfig,
    formatter::{formatter, Formatter},
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use fluvio::{Offset, RecordKey, TopicProducer};
use fluvio_connector_common::{tracing::error, Source};
use futures::{stream::LocalBoxStream, StreamExt};
use reqwest::{Client, RequestBuilder, Url};
use tokio::time::Interval;
use tokio_stream::wrappers::IntervalStream;

use std::sync::Arc;

pub(crate) struct HttpSource {
    interval: Interval,
    pub request: RequestBuilder,
    formatter: Arc<dyn Formatter + Sync + Send>,
    pub delimiter: String,
}

impl HttpSource {
    pub(crate) fn new(config: &HttpConfig) -> Result<Self> {
        let client = Client::new();
        let method = config.method.parse()?;
        let url = Url::parse(&config.endpoint.resolve()?)
            .context("unable to parse http endpoint")?;

        let mut request = client.request(method, url);

        request = request
            .header(reqwest::header::USER_AGENT, config.user_agent.clone());
        let headers = config
            .headers
            .iter()
            .map(|h| h.resolve().unwrap_or_default())
            .collect::<Vec<_>>();

        for (key, value) in headers.iter().flat_map(|h| h.split_once(':')) {
            request = request.header(key, value);
        }
        if let Some(ref body) = config.body {
            request = request.body(body.clone());
        }

        let interval = tokio::time::interval(config.interval);
        let formatter = formatter(config.output_type, config.output_parts);
        let delimiter = config.delimiter.clone();
        Ok(Self {
            interval,
            request,
            formatter,
            delimiter,
        })
    }

    pub async fn http_response_stream(
        &self,
    ) -> Result<LocalBoxStream<Result<bytes::Bytes, reqwest::Error>>> {
        let request_builder = match self.request.try_clone() {
            Some(builder) => builder,
            None => {
                return Err(anyhow!("Request must be cloneable"));
            }
        };

        let response = match request_builder.send().await {
            Ok(response) => response,
            Err(e) => {
                error!(%e);
                return Err(anyhow!("unable to open http stream"));
            }
        };

        let res = response.bytes_stream().boxed_local();

        Ok(res)
    }

    pub async fn produce_streaming_data<'a>(
        &self,
        http_chunk_stream: LocalBoxStream<'a, Result<bytes::Bytes, reqwest::Error>>,
        producer: TopicProducer,
    ) -> Result<()> {
        let mut record_stream =
            record_stream(http_chunk_stream, self.delimiter.clone());

        while let Some(chunk) = record_stream.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(err) => {
                    error!("Chunk retrieval failed: {}", err);
                    continue;
                }
            };

            println!("sending record to producer: {:?}", chunk);
            producer.send(RecordKey::NULL, chunk).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl<'a> Source<'a, String> for HttpSource {
    async fn connect(
        self,
        _offset: Option<Offset>,
    ) -> Result<LocalBoxStream<'a, String>> {
        let stream = IntervalStream::new(self.interval).filter_map(move |_| {
            let builder = self.request.try_clone();
            let formatter = self.formatter.clone();
            async move {
                match request(builder, formatter.as_ref()).await {
                    Ok(res) => Some(res),
                    Err(err) => {
                        error!("Request execution failed: {}", err);
                        None
                    }
                }
            }
        });
        Ok(stream.boxed_local())
    }
}

async fn request(
    builder: Option<RequestBuilder>,
    formatter: &dyn Formatter,
) -> Result<String> {
    let request =
        builder.ok_or_else(|| anyhow!("Request must be cloneable"))?;
    let response = request.send().await.context("Request failed")?;
    println!("Response: {:?}", response);

    formatter.to_string(response).await
}
