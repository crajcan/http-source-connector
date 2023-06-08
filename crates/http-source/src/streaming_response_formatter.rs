use crate::formatter::{Formatter, HttpResponseRecord};
use bytes::BytesMut;
use std::sync::Arc;

#[derive(Clone)]
pub struct StreamingResponseFormatter {
    formatter: Arc<dyn Formatter + Sync + Send>,
    http_response_record: HttpResponseRecord,
}

impl StreamingResponseFormatter {
    pub(crate) fn new(
        formatter: Arc<dyn Formatter + Sync + Send>,
        http_response_record: HttpResponseRecord
    ) -> anyhow::Result<Self> {


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
