use crate::config::{OutputParts, OutputType};
use crate::{formatter::ResponseFormatter, HttpSource};
use anyhow::Result;
use async_trait::async_trait;
use fluvio::Offset;
use fluvio_connector_common::{tracing::error, Source};
use futures::{stream::LocalBoxStream, StreamExt};
use tokio_stream::wrappers::IntervalStream;

pub(crate) struct PollingSource {
    source: HttpSource,
    formatter: ResponseFormatter,
}

impl PollingSource {
    pub fn new(source: HttpSource) -> Self {
        let formatter =
            ResponseFormatter::new(source.output_type, source.output_parts);

        Self { source, formatter }
    }
}

#[async_trait]
impl<'a> Source<'a, String> for PollingSource {
    async fn connect(
        self,
        _offset: Option<Offset>,
    ) -> Result<LocalBoxStream<'a, String>> {
        let stream =
            IntervalStream::new(self.source.interval).filter_map(move |_| {
                let request_builder = self.source.request.try_clone();
                let formatter = self.formatter.clone();

                async move {
                    let next_request = match request_builder {
                        Some(builder) => builder,
                        None => {
                            error!("Failed to clone request builder");
                            return None;
                        }
                    };

                    let response = match next_request.send().await {
                        Ok(res) => res,
                        Err(err) => {
                            error!("Request execution failed: {}", err);
                            return None;
                        }
                    };

                    match formatter.response_to_string(response).await {
                        Ok(record) => Some(record),
                        Err(err) => {
                            error!("Failed to format response: {}", err);
                            None
                        }
                    }
                }
            });

        Ok(stream.boxed_local())
    }
}
