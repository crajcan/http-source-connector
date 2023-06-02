use crate::streaming_response_formatter::StreamingResponseFormatter;
use bstr::ByteSlice;
use bytes::{Bytes, BytesMut};
use futures::{stream::LocalBoxStream, StreamExt};
use std::sync::{Arc, Mutex};

pub(crate) fn record_stream<'a>(
    stream: LocalBoxStream<'a, Result<bytes::Bytes, reqwest::Error>>,
    delimiter: String,
    formatter: StreamingResponseFormatter,
) -> LocalBoxStream<'a, Result<bytes::Bytes, reqwest::Error>> {
    let buffer = Arc::new(Mutex::new(BytesMut::new()));

    let res = stream.filter_map(move |mut received_chunk| {
        let buffer = Arc::clone(&buffer);
        let delimiter = delimiter.as_bytes().to_vec();
        let formatter = formatter.clone();

        async move {
            match received_chunk {
                Ok(ref mut received_chunk) => {
                    let mut buffer = buffer.lock().unwrap();

                    let mut chunk_with_remainder_prepended = buffer.clone();
                    chunk_with_remainder_prepended
                        .extend_from_slice(received_chunk);

                    buffer.clear();

                    let split_chunk: Vec<&[u8]> =
                        chunk_with_remainder_prepended
                            .split_str(&delimiter)
                            .collect();

                    if let Some((remainder, records)) = split_chunk.split_last()
                    {
                        let mut result_chunk = None;

                        if !records.is_empty() {
                            let joined_records = &bstr::join("\n", records);
                            let mut result_bytes = BytesMut::new();
                            result_bytes.extend_from_slice(joined_records);

                            let formatted =
                                formatter.format(result_bytes.clone()).ok()?;
                            result_bytes.clear();
                            result_bytes.extend_from_slice(formatted.as_bytes());

                            result_chunk = Some(Ok(result_bytes.freeze()));
                        }

                        buffer.extend_from_slice(remainder);

                        result_chunk
                    } else {
                        None
                    }
                }
                Err(err) => Some(Err(err)),
            }
        }
    });

    res.boxed_local()
}

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

#[cfg(test)]
mod test {
    use futures::StreamExt;
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

        let mut chunked_stream = super::record_stream(boxed, "!".to_string());

        let first_chunk = chunked_stream.next().await;
        assert_eq!(
            first_chunk.unwrap().unwrap(),
            bytes::Bytes::from("Hello world")
        );

        let second_chunk = chunked_stream.next().await;
        assert_eq!(
            second_chunk.unwrap().unwrap(),
            bytes::Bytes::from(" Welcome to NY")
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

        let mut chunked_stream = super::record_stream(boxed, "!".to_string());

        let first_chunk = chunked_stream.next().await;
        assert_eq!(
            first_chunk.unwrap().unwrap(),
            bytes::Bytes::from("Hello world")
        );

        let second_chunk = chunked_stream.next().await;
        assert_eq!(
            second_chunk.unwrap().unwrap(),
            bytes::Bytes::from(" Welcome to NY")
        );
    }

    #[async_std::test]
    async fn test_record_stream_handles_multi_record_chunks() {
        let inner_stream = futures::stream::iter(vec![Ok(bytes::Bytes::from(
            "Hello world! Welcome to NY! Glad you coul",
        ))]);
        let boxed = inner_stream.boxed_local();

        let mut chunked_stream = super::record_stream(boxed, "!".to_string());

        let first_chunk = chunked_stream.next().await;
        assert_eq!(
            first_chunk.unwrap().unwrap(),
            bytes::Bytes::from("Hello world\n Welcome to NY")
        );
    }

    #[async_std::test]
    async fn test_record_stream_handles_chunks_beginning_with_delimiter() {
        let inner_stream = futures::stream::iter(vec![
            Ok(bytes::Bytes::from("Hello world")),
            Ok(bytes::Bytes::from("! Welcome to NY!")),
        ]);
        let boxed = inner_stream.boxed_local();

        let mut chunked_stream = super::record_stream(boxed, "!".to_string());

        let first_chunk = chunked_stream.next().await;
        assert_eq!(
            first_chunk.unwrap().unwrap(),
            bytes::Bytes::from("Hello world\n Welcome to NY")
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

        let mut chunked_stream = super::record_stream(boxed, "!".to_string());

        let first_chunk = chunked_stream.next().await;
        assert_eq!(
            first_chunk.unwrap().unwrap(),
            bytes::Bytes::from("Hello world")
        );

        let second_chunk = chunked_stream.next().await;
        assert_eq!(
            second_chunk.unwrap().unwrap(),
            bytes::Bytes::from(" Welcome to NY")
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
}
