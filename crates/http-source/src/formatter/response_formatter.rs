use anyhow::Result;

use crate::config::{OutputParts, OutputType};

use super::{
    http_response_record::HttpResponseRecord, JsonFormatter, TextFormatter,
};

#[derive(Clone)]
pub(crate) enum ResponseFormatter {
    JsonFormatter(JsonFormatter),
    TextFormatter(TextFormatter),
}

impl ResponseFormatter {
    pub fn new(output_type: OutputType, output_parts: OutputParts) -> Self {
        match output_type {
            OutputType::Json => {
                Self::JsonFormatter(JsonFormatter::new(output_parts))
            }
            OutputType::Text => {
                Self::TextFormatter(TextFormatter::new(output_parts))
            }
        }
    }

    pub async fn response_to_string(
        &self,
        response: reqwest::Response,
    ) -> Result<String> {
        let record = HttpResponseRecord::record_from_response(response).await?;

        self.record_to_string(record)
    }

    pub fn record_to_string(
        &self,
        record: HttpResponseRecord,
    ) -> Result<String> {
        match self {
            Self::JsonFormatter(formatter) => {
                formatter.response_to_string(record)
            }
            Self::TextFormatter(formatter) => {
                formatter.response_to_string(record)
            }
        }
    }
}
