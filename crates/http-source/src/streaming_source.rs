use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use fluvio::Offset;
use fluvio_connector_common::{tracing::error, Source};
use futures::{stream::LocalBoxStream, StreamExt};
use reqwest::Response;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::{formatter::StreamFormatter, HttpSource};

pub(crate) struct StreamingSource {
    source: HttpSource,
    initial_response: Response,
    formatter: StreamFormatter,
}

impl StreamingSource {
    pub(crate) fn new(
        source: HttpSource,
        initial_response: Response,
    ) -> Result<Self> {
        let formatter = StreamFormatter::new(
            &initial_response,
            source.output_type,
            source.output_parts,
        )?;

        Ok(Self {
            source,
            initial_response,
            formatter,
        })
    }

    /// Sends the http request and returns a stream of the response body
    pub(crate) async fn http_response_bytes_stream<'a>(
        self,
    ) -> Result<LocalBoxStream<'a, Result<bytes::Bytes, reqwest::Error>>> {
        let res = self.initial_response.bytes_stream().boxed_local();

        Ok(res)
    }
}

#[async_trait]
impl<'a> Source<'a, Result<bytes::Bytes, reqwest::Error>> for StreamingSource {
    async fn connect(
        self,
        _offset: Option<Offset>,
    ) -> Result<LocalBoxStream<'a, Result<bytes::Bytes, reqwest::Error>>> {
        let delimiter = self.source.delimiter.as_bytes().to_vec();
        let formatter = Arc::new(self.formatter.clone());

        let bytes_stream = self.http_response_bytes_stream().await?;

        Ok(record_stream(bytes_stream, delimiter, formatter))
    }
}

pub(crate) fn record_stream<'a>(
    stream: LocalBoxStream<'a, Result<bytes::Bytes, reqwest::Error>>,
    delimiter: Vec<u8>,
    formatter: Arc<StreamFormatter>,
) -> LocalBoxStream<'a, Result<bytes::Bytes, reqwest::Error>> {
    let buffer = Arc::new(Mutex::new(BytesMut::new()));

    let res = stream.filter_map(move |mut received_chunk| {
        let buffer = Arc::clone(&buffer);
        let delimiter = delimiter.clone();
        let formatter = formatter.clone();

        async move {
            match received_chunk {
                Ok(ref mut received_chunk) => {
                    let mut buf = buffer.lock().unwrap();
                    buf.extend_from_slice(&received_chunk);

                    dequeue_records(&mut buf, &delimiter, formatter)
                }
                Err(err) => {
                    println!("returning a Some(Err): {:?}", err);

                    Some(Err(err))
                }
            }
        }
    });

    res.boxed_local()
}

fn dequeue_records(
    mut buf: &mut MutexGuard<BytesMut>,
    delimiter: &[u8],
    formatter: Arc<StreamFormatter>,
) -> Option<Result<bytes::Bytes, reqwest::Error>> {
    let mut result_bytes = BytesMut::new();

    while let Some(index) = first_delim_index(&buf, &delimiter) {
        let next_record = next_record(&mut buf, index, &delimiter);

        let formatted_record =
            match formatter.streaming_record_to_string(next_record) {
                Ok(record) => record,
                Err(err) => {
                    error!("formatter.format() failed: {:?}", err);
                    return None;
                }
            };

        result_bytes.extend_from_slice(&formatted_record.as_bytes());
        result_bytes.put(&b"\n"[..]);
    }

    if result_bytes.is_empty() {
        return None;
    }

    Some(Ok(result_bytes.freeze()))
}

fn next_record(
    buffer: &mut MutexGuard<BytesMut>,
    index: usize,
    delimiter: &[u8],
) -> BytesMut {
    let mut next_record = buffer.split_to(index + delimiter.len());

    next_record.truncate(next_record.len() - delimiter.len());

    next_record
}

fn first_delim_index(bytes: &[u8], delimiter: &[u8]) -> Option<usize> {
    if delimiter.len() == 0 {
        return None;
    }

    if bytes.len() < delimiter.len() {
        return None;
    }

    let mut i = 0;
    while i < bytes.len() {
        if bytes[i..].starts_with(delimiter) {
            return Some(i);
        }

        i = i + 1;
    }

    None
}

#[cfg(test)]
mod test {
    use crate::config::{OutputParts, OutputType};
    use crate::formatter::{formatter, HttpResponseRecord};
    use crate::streaming_response_formatter::StreamingResponseFormatter;
    use futures::StreamExt;

    fn mock_streaming_formatter() -> StreamingResponseFormatter {
        let formatter = formatter(OutputType::Text, OutputParts::Body);
        let http_response_record = HttpResponseRecord::default();

        super::StreamingResponseFormatter::new(formatter, http_response_record)
            .unwrap()
    }

    #[test]
    fn my_test_bytes() {
        use bytes::{BufMut, BytesMut};

        let mut buf = BytesMut::with_capacity(1024);
        buf.put(&b"hello world"[..]);
        buf.put_u16(1234);

        let a = buf.split();
        let c = b"hello world\x04\xD2";
        let d = &c[..];

        assert_eq!(a, d);

        buf.put(&b"goodbye world"[..]);

        let b = buf.split();
        assert_eq!(b, b"goodbye world"[..]);

        assert_eq!(buf.capacity(), 998);
    }

    #[async_std::test]
    async fn test_record_stream_concatenates_chunks() {
        let inner_stream = futures::stream::iter(vec![
            Ok(bytes::Bytes::from("Hello")),
            Ok(bytes::Bytes::from(" world")),
            Ok(bytes::Bytes::from("!")),
            Ok(bytes::Bytes::from(" Welcome")),
            Ok(bytes::Bytes::from(" to ")),
            Ok(bytes::Bytes::from("NY!")),
        ]);
        let boxed = inner_stream.boxed_local();

        let mut chunked_stream =
            super::record_stream(boxed, vec![b'!'], mock_streaming_formatter());

        let first_chunk = chunked_stream.next().await;
        assert_eq!(
            first_chunk.unwrap().unwrap(),
            bytes::Bytes::from("Hello world\n")
        );

        let second_chunk = chunked_stream.next().await;
        assert_eq!(
            second_chunk.unwrap().unwrap(),
            bytes::Bytes::from(" Welcome to NY\n")
        );
    }

    #[async_std::test]
    async fn test_record_stream_handles_remainders() {
        let inner_stream = futures::stream::iter(vec![
            Ok(bytes::Bytes::from("Hello wo")),
            Ok(bytes::Bytes::from("rld! Wel")),
            Ok(bytes::Bytes::from("come to NY!")),
        ]);
        let boxed = inner_stream.boxed_local();

        let mut chunked_stream =
            super::record_stream(boxed, vec![b'!'], mock_streaming_formatter());

        let first_chunk = chunked_stream.next().await;
        assert_eq!(
            first_chunk.unwrap().unwrap(),
            bytes::Bytes::from("Hello world\n")
        );

        let second_chunk = chunked_stream.next().await;
        assert_eq!(
            second_chunk.unwrap().unwrap(),
            bytes::Bytes::from(" Welcome to NY\n")
        );
    }

    #[async_std::test]
    async fn test_record_stream_handles_multi_record_chunks() {
        let inner_stream = futures::stream::iter(vec![Ok(bytes::Bytes::from(
            "Hello world! Welcome to NY! Glad you coul",
        ))]);
        let boxed = inner_stream.boxed_local();

        let mut chunked_stream =
            super::record_stream(boxed, vec![b'!'], mock_streaming_formatter());

        let first_chunk = chunked_stream.next().await;
        assert_eq!(
            first_chunk.unwrap().unwrap(),
            bytes::Bytes::from("Hello world\n Welcome to NY\n")
        );
    }

    #[async_std::test]
    async fn test_record_stream_handles_chunks_beginning_with_delimiter() {
        let inner_stream = futures::stream::iter(vec![
            Ok(bytes::Bytes::from("Hello world")),
            Ok(bytes::Bytes::from("! Welcome to NY!")),
        ]);
        let boxed = inner_stream.boxed_local();

        let mut chunked_stream =
            super::record_stream(boxed, vec![b'!'], mock_streaming_formatter());

        let first_chunk = chunked_stream.next().await;
        assert_eq!(
            first_chunk.unwrap().unwrap(),
            bytes::Bytes::from("Hello world\n Welcome to NY\n")
        );
    }

    #[async_std::test]
    async fn test_record_stream_handles_chunks_ending_with_delimiter() {
        let inner_stream = futures::stream::iter(vec![
            Ok(bytes::Bytes::from("Hello wo")),
            Ok(bytes::Bytes::from("rld!")),
            Ok(bytes::Bytes::from(" Welcome to NY!")),
        ]);
        let boxed = inner_stream.boxed_local();

        let mut chunked_stream =
            super::record_stream(boxed, vec![b'!'], mock_streaming_formatter());

        let first_chunk = chunked_stream.next().await;
        assert_eq!(
            first_chunk.unwrap().unwrap(),
            bytes::Bytes::from("Hello world\n")
        );

        let second_chunk = chunked_stream.next().await;
        assert_eq!(
            second_chunk.unwrap().unwrap(),
            bytes::Bytes::from(" Welcome to NY\n")
        );
    }

    #[test]
    fn test_last_delim_index_finds_single_byte_delimiters() {
        assert_eq!(super::last_delim_index(b"", b"\n"), None);
        assert_eq!(super::last_delim_index(b"0", b"\n"), None);
        assert_eq!(super::last_delim_index(b"\n", b"\n"), Some(0));
        assert_eq!(super::last_delim_index(b"0\n", b"\n"), Some(1));
        assert_eq!(super::last_delim_index(b"\n2", b"\n"), Some(0));
        assert_eq!(super::last_delim_index(b"\n2\n", b"\n"), Some(2));
        assert_eq!(super::last_delim_index(b"012345", b"\n"), None);
        assert_eq!(super::last_delim_index(b"0123\n6", b"\n"), Some(4));
        assert_eq!(super::last_delim_index(b"0123\n5\n", b"\n"), Some(6));
        assert_eq!(super::last_delim_index(b"0123\n56\n", b"\n"), Some(7));
    }

    #[test]
    fn test_last_delim_index_finds_multi_byte_delimiters() {
        assert_eq!(super::last_delim_index(b"", b",\n"), None);
        assert_eq!(super::last_delim_index(b"0", b",\n"), None);
        assert_eq!(super::last_delim_index(b",\n", b",\n"), Some(0));
        assert_eq!(super::last_delim_index(b"0,\n", b",\n"), Some(1));
        assert_eq!(super::last_delim_index(b",\n2", b",\n"), Some(0));
        assert_eq!(super::last_delim_index(b",\n2,\n", b",\n"), Some(3));
        assert_eq!(super::last_delim_index(b"012345", b",\n"), None);
        assert_eq!(super::last_delim_index(b"0123,\n6", b",\n"), Some(4));
        assert_eq!(super::last_delim_index(b"0123,\n6,\n", b",\n"), Some(7));
        assert_eq!(super::last_delim_index(b"0123,\n67,\n", b",\n"), Some(8));
    }

    #[test]
    fn test_first_delim_index_finds_single_byte_delimiters() {
        assert_eq!(super::first_delim_index(b"", b"\n"), None);
        assert_eq!(super::first_delim_index(b"0", b"\n"), None);
        assert_eq!(super::first_delim_index(b"\n", b"\n"), Some(0));
        assert_eq!(super::first_delim_index(b"0\n", b"\n"), Some(1));
        assert_eq!(super::first_delim_index(b"\n2", b"\n"), Some(0));
        assert_eq!(super::first_delim_index(b"\n2\n", b"\n"), Some(0));
        assert_eq!(super::first_delim_index(b"012345", b"\n"), None);
        assert_eq!(super::first_delim_index(b"0123\n6", b"\n"), Some(4));
        assert_eq!(super::first_delim_index(b"0123\n5\n", b"\n"), Some(4));
        assert_eq!(super::first_delim_index(b"0123\n56\n", b"\n"), Some(4));
        assert_eq!(super::first_delim_index(b"0123\n56\n8", b"\n"), Some(4));
    }

    #[test]
    fn test_first_delim_index_finds_multi_bytes_delimiters() {
        assert_eq!(super::first_delim_index(b"", b",\n"), None);
        assert_eq!(super::first_delim_index(b"0", b",\n"), None);
        assert_eq!(super::first_delim_index(b",\n", b",\n"), Some(0));
        assert_eq!(super::first_delim_index(b"0,\n", b",\n"), Some(1));
        assert_eq!(super::first_delim_index(b",\n2", b",\n"), Some(0));
        assert_eq!(super::first_delim_index(b",\n2,\n", b",\n"), Some(0));
        assert_eq!(super::first_delim_index(b"012345", b",\n"), None);
        assert_eq!(super::first_delim_index(b"0123,\n6", b",\n"), Some(4));
        assert_eq!(super::first_delim_index(b"0123,\n6,\n", b",\n"), Some(4));
        assert_eq!(super::first_delim_index(b"0123,\n67,\n", b",\n"), Some(4));
        assert_eq!(super::first_delim_index(b"0123,\n67,\n8", b",\n"), Some(4));
    }
}

// buffer.extend_from_slice(&received_chunk);

// let mut result_records = BytesMut::new();
// let mut i = 0;
// let mut next_record = Vec::new();
// println!("buffer: {:?}", buffer);
// println!("buffer.len(): {}", buffer.len());
// while i < buffer.len() {
//     if buffer[i..].starts_with(&delimiter) {
//         println!("buffer starts with needle");
//         println!("next_record: {:?}", next_record);
//         println!("i: {:?}", i);

//         if !(i == buffer.len() - 1) {
//             next_record.push(b'\n');
//         }

//         if !next_record.is_empty() {
//             result_records.extend_from_slice(&next_record);
//             next_record.clear();
//         }

//         i = i + delimiter.len();
//     } else {
//         next_record.push(buffer[i]);
//         i = i + 1;
//     }
// }

// buffer.clear();
// buffer.extend_from_slice(&next_record);

// if result_records.is_empty() {
//     None
// } else {
//     Some(Ok(result_records.freeze()))
// }

// let mut buffer = buffer.lock().unwrap();
// buffer.extend_from_slice(received_chunk);

// let split_chunk: Vec<&[u8]> =
//     buffer.split_str(&delimiter).collect();

// if let Some((remainder, records)) = split_chunk.split_last()
// {
//     let mut result_chunk = None;

//     if !records.is_empty() {
//         let joined_records = &bstr::join("\n", records);
//         let mut result_bytes = BytesMut::new();
//         result_bytes.extend_from_slice(joined_records);

//         result_chunk = Some(Ok(result_bytes.freeze()));

//         buffer.clear();
//     }

//     buffer.extend_from_slice(remainder);

//     result_chunk
// } else {
//     None
// }%

#[allow(dead_code)]
fn last_delim_index(bytes: &[u8], delimiter: &[u8]) -> Option<usize> {
    if delimiter.len() == 0 {
        return None;
    }

    if bytes.len() < delimiter.len() {
        return None;
    }

    let mut end = bytes.len();
    while end >= delimiter.len() {
        if bytes[0..end].ends_with(delimiter) {
            return Some(end - delimiter.len());
        }

        end = end - 1;
    }

    None
}
