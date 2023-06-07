use crate::{
    config::HttpConfig,
    formatter::{formatter, Formatter},
};

use anyhow::{anyhow, Context, Result};
use fluvio_connector_common::tracing::error;
use reqwest::{Client, RequestBuilder, Url};
use tokio::time::Interval;

use std::sync::Arc;

pub(crate) struct HttpSource {
    pub(crate) interval: Interval,
    pub request: RequestBuilder,
    pub formatter: Arc<dyn Formatter + Sync + Send>,
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


    pub async fn issue_request(
        &self
    ) -> Result<reqwest::Response> {
        let request = self
            .request
            .try_clone()
            .context("unable to clone request builder")?;

        match request.send().await {
            Ok(response) => Ok(response),
            Err(e) => {
                error!(%e);
                Err(anyhow!("Http request failed"))
            }
        }
    }
}
