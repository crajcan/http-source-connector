use crate::http_response_stream::HttpResponseStream;
use crate::{
    config::HttpConfig,
    formatter::{formatter, Formatter},
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use bytes::BytesMut;
use fluvio::{Offset, RecordKey, TopicProducer};
use fluvio_connector_common::{tracing::error, Source};
use futures::{stream::LocalBoxStream, StreamExt};
use futures_util::stream::Stream;
use reqwest::{Client, RequestBuilder, Url};
use std::pin::Pin;
use tokio::time::Interval;
use tokio_stream::wrappers::IntervalStream;

use std::sync::{Arc, Mutex};

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

    pub async fn streaming_response(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<bytes::Bytes, reqwest::Error>>>>>
    {
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
        let delimiter = self.delimiter.as_bytes().to_vec();
        println!("delimiter: {:?}", delimiter);

        Ok(res)
    }

    pub fn chunk_http_stream<'a>(
        self,
        stream: futures::stream::LocalBoxStream<
            'a,
            Result<bytes::Bytes, reqwest::Error>,
        >,
    ) -> LocalBoxStream<'a, Result<bytes::Bytes, reqwest::Error>> {
        let buffer = Arc::new(Mutex::new(BytesMut::new()));

        let res = stream.filter_map(move |chunk_result| {
            let buffer = Arc::clone(&buffer);
            let delimiter = self.delimiter.as_bytes().to_vec();

            async move {
                match chunk_result {
                    Ok(ref chunk) => {
                        let chunk_bytes = chunk.as_ref();
                        let mut buffer = buffer.lock().unwrap();
                        buffer.extend_from_slice(chunk_bytes);

                        if chunk_bytes.ends_with(&delimiter) {
                            let full_chunk = buffer.clone();
                            buffer.clear();

                            Some(Ok(full_chunk.freeze()))
                        } else {
                            None
                        }
                    }
                    Err(err) => Some(Err(err)),
                }
            }
        });

        res.boxed_local()
    }

    pub async fn produce_streaming_data<'a>(
        self,
        stream: LocalBoxStream<'a, Result<bytes::Bytes, reqwest::Error>>,
        producer: TopicProducer,
    ) -> Result<()> {
        let mut new_stream = self.chunk_http_stream(stream);

        while let Some(chunk) = new_stream.next().await {
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
