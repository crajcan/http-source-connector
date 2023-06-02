use crate::formatter::{Formatter, HttpResponseRecord};
use anyhow::Context;
use bytes::BytesMut;
use reqwest::Response;
use std::sync::Arc;

#[derive(Clone)]
pub struct StreamingResponseFormatter {
    formatter: Arc<dyn Formatter + Sync + Send>,
    http_response_record: HttpResponseRecord,
}

impl StreamingResponseFormatter {
    pub(crate) fn new(
        formatter: Arc<dyn Formatter + Sync + Send>,
        response: Response,
    ) -> anyhow::Result<Self> {
        let http_response_record = HttpResponseRecord::try_from(&response)
            .context("unable to convert http response to record")?;

        Ok(Self {
            formatter,
            http_response_record,
        })
    }

    pub fn format(&self, chunk: BytesMut) -> anyhow::Result<String> {
        let formatted = self.formatter.streaming_record_to_string(
            self.http_response_record.clone(),
            chunk,
        );

        formatted
    }
}
