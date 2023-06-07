use crate::{
    formatter::HttpResponseRecord, record_stream::record_stream,
    streaming_response_formatter::StreamingResponseFormatter, HttpSource,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use fluvio::Offset;
use fluvio_connector_common::{tracing::error, Source};
use futures::{stream::LocalBoxStream, StreamExt};
use reqwest::Response;

pub(crate) struct StreamingSource {
    source: HttpSource,
    streaming_response_formatter: StreamingResponseFormatter,
}

impl StreamingSource {
    pub(crate) fn new(
        source: HttpSource,
        initial_response: Response,
    ) -> Result<Self> {
        let http_response_record = HttpResponseRecord::try_from(
            &initial_response,
        )
        .context("unable to convert http response to HttpResponseRecord")?;

        let streaming_response_formatter = StreamingResponseFormatter::new(
            source.formatter.clone(),
            http_response_record,
        )?;

        Ok(Self {
            source,
            streaming_response_formatter,
        })
    }

    /// Sends the http request and returns a stream of the response body
    pub(crate) async fn http_response_bytes_stream<'a>(
        &self,
    ) -> Result<LocalBoxStream<'a, Result<bytes::Bytes, reqwest::Error>>> {
        let response = match self.source.issue_request().await {
            Ok(response) => response,
            Err(e) => {
                error!(%e);
                return Err(anyhow!("unable to open http stream"));
            }
        };

        let res = response.bytes_stream().boxed_local();

        Ok(res)
    }
}

#[async_trait]
impl<'a> Source<'a, Result<bytes::Bytes, reqwest::Error>> for StreamingSource {
    async fn connect(
        self,
        _offset: Option<Offset>,
    ) -> Result<LocalBoxStream<'a, Result<bytes::Bytes, reqwest::Error>>> {
        Ok(record_stream(
            self.http_response_bytes_stream().await?,
            self.source.delimiter,
            self.streaming_response_formatter,
        ))
    }
}
