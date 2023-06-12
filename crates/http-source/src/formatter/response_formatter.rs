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

#[cfg(test)]
mod test {
    use super::ResponseFormatter;
    use crate::config::{OutputParts, OutputType};
    use anyhow::Result;
    use mockito::{mock, server_url, Mock};
    use reqwest::{Client, Response};

    #[async_std::test]
    async fn test_full_text_output() -> Result<()> {
        //given
        let response = send_request().await?;
        let formatter =
            ResponseFormatter::new(OutputType::Text, OutputParts::Full);

        //when
        let string = formatter.response_to_string(response).await?;

        //then
        assert_eq!(
            string,
            "HTTP/1.1 201 Created\nconnection: close\ncontent-type: text/plain\nx-api-key: 1234\nx-api-attribute: a1\nx-api-attribute: a2\ncontent-length: 5\n\nworld"
        );
        Ok(())
    }

    #[async_std::test]
    async fn test_body_text_output() -> Result<()> {
        //given
        let response = send_request().await?;
        let formatter =
            ResponseFormatter::new(OutputType::Text, OutputParts::Body);
        //when
        let string = formatter.response_to_string(response).await?;

        //then
        assert_eq!(string, "world");
        Ok(())
    }

    #[async_std::test]
    async fn test_full_json_output() -> Result<()> {
        //given
        let response = send_request().await?;
        let formatter =
            ResponseFormatter::new(OutputType::Json, OutputParts::Full);

        //when
        let string = formatter.response_to_string(response).await?;

        //then
        assert_eq!(
            string,
            r#"{"status":{"version":"HTTP/1.1","code":201,"string":"Created"},"header":{"connection":"close","content-length":"5","content-type":"text/plain","x-api-attribute":["a1","a2"],"x-api-key":"1234"},"body":"world"}"#
        );
        Ok(())
    }

    #[async_std::test]
    async fn test_body_json_output() -> Result<()> {
        //given
        let response = send_request().await?;
        let formatter =
            ResponseFormatter::new(OutputType::Json, OutputParts::Body);

        //when
        let string = formatter.response_to_string(response).await?;

        //then
        assert_eq!(string, r#"{"body":"world"}"#);
        Ok(())
    }

    #[async_std::test]
    async fn test_unparsable_header() -> Result<()> {
        //given
        let mock = mock("GET", "/bad")
            .with_status(201)
            .with_header("bad-header", "🦄")
            .create();
        let client = Client::new();
        let response = client
            .request("GET".parse()?, format!("{}/bad", server_url()))
            .send()
            .await?;
        let formatter =
            ResponseFormatter::new(OutputType::Json, OutputParts::Full);

        //when
        let res = formatter.response_to_string(response).await;

        //then
        mock.assert();
        assert_eq!(
            res.unwrap_err().to_string(),
            "Failed to read response headers"
        );
        Ok(())
    }

    async fn send_request() -> Result<Response> {
        let (url, mock) = create_mock();
        let client = Client::new();
        let request = client.request("GET".parse()?, format!("{url}/hello"));
        let response = request.send().await?;
        mock.assert();
        Ok(response)
    }

    fn create_mock() -> (String, Mock) {
        (
            server_url(),
            mock("GET", "/hello")
                .with_status(201)
                .with_header("content-type", "text/plain")
                .with_header("x-api-key", "1234")
                .with_header("x-api-attribute", "a1")
                .with_header("x-api-attribute", "a2")
                .with_body("world")
                .create(),
        )
    }
}
