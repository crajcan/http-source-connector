use serde::Serialize;
use std::collections::{btree_map::Entry, BTreeMap};

use super::http_response_record::{HttpHeader, HttpResponseRecord};

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct HttpJsonRecord {
    #[serde(skip_serializing_if = "Option::is_none")] status: Option<HttpJsonStatus>, #[serde(skip_serializing_if = "Option::is_none")]
    header: Option<BTreeMap<String, JsonHeadersValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct HttpJsonStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    string: Option<&'static str>,
}

impl From<HttpResponseRecord> for HttpJsonRecord {
    fn from(resp_record: HttpResponseRecord) -> Self {
        let HttpResponseRecord {
            version,
            status_code,
            status_string,
            headers,
            body,
        } = resp_record;

        let header = headers.map(headers_to_json);

        let status = match (&version, &status_code, &status_string) {
            (None, None, None) => None,
            _ => Some(HttpJsonStatus {
                version,
                code: status_code,
                string: status_string,
            }),
        };

        HttpJsonRecord {
            status,
            header,
            body,
        }
    }
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(untagged)]
enum JsonHeadersValue {
    One(String),
    Many(Vec<String>),
}

impl JsonHeadersValue {
    fn push(&mut self, value: String) {
        match self {
            JsonHeadersValue::One(_) => {
                let prev = std::mem::replace(
                    self,
                    JsonHeadersValue::Many(Vec::with_capacity(2)),
                );
                if let (Self::One(prev_value), Self::Many(vec)) = (prev, self) {
                    vec.push(prev_value);
                    vec.push(value);
                }
            }
            JsonHeadersValue::Many(vec) => vec.push(value),
        }
    }
}

fn headers_to_json(
    headers: Vec<HttpHeader>,
) -> BTreeMap<String, JsonHeadersValue> {
    let mut result: BTreeMap<String, JsonHeadersValue> = BTreeMap::new();
    for header in headers {
        match result.entry(header.name) {
            Entry::Occupied(mut entry) => {
               let foo = entry.get_mut();
               foo.push(header.value);
            },
            Entry::Vacant(entry) => {
                entry.insert(JsonHeadersValue::One(header.value));
            }
        };
    }
    result
}
