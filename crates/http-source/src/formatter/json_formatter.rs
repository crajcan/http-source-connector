use crate::config::OutputParts;
use anyhow::Result;

use super::{
    http_json_record::HttpJsonRecord, http_response_record::HttpResponseRecord,
};

#[derive(Clone)]
pub(crate) struct JsonFormatter(OutputParts);

impl JsonFormatter {
    pub fn new(output_parts: OutputParts) -> Self {
        Self(output_parts)
    }

    pub fn response_to_string(
        &self,
        record: HttpResponseRecord
    ) -> anyhow::Result<String> {
        let json_record = match self.0 {
            OutputParts::Body => HttpJsonRecord::from(HttpResponseRecord {
                body: record.body,
                ..Default::default()
            }),
            OutputParts::Full => HttpJsonRecord::from(record),
        };

        Ok(serde_json::to_string(&json_record)?)
    }
}
