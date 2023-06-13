use crate::config::{OutputParts, OutputType};
use anyhow::{Context, Result};
use bytes::{BytesMut, Bytes};

use super::{
    http_response_record::HttpResponseRecord,
    response_formatter::ResponseFormatter,
};

#[derive(Clone)]
pub(crate) struct StreamFormatter {
    http_response_record: HttpResponseRecord,
    response_formatter: ResponseFormatter,
}

impl StreamFormatter {
    pub fn new(
        response: &reqwest::Response,
        output_type: OutputType,
        output_parts: OutputParts,
    ) -> Result<Self> {
        let http_response_record = HttpResponseRecord::try_from(response)
            .context("unable to convert http response to record")?;

        let response_formatter =
            ResponseFormatter::new(output_type, output_parts);

        Ok(Self {
            http_response_record,
            response_formatter,
        })
    }

    pub fn streaming_record_to_string(
        &mut self,
        next_record: Bytes,
    ) -> Result<String> {
        self.http_response_record.body = Some(next_record);

        self.response_formatter
            .record_to_string(&self.http_response_record)
    }
}
