use bytes::BytesMut;
use futures::{stream::LocalBoxStream, StreamExt};
use std::sync::{Arc, Mutex};

pub fn accumulator_stream<'a>(
    stream: LocalBoxStream<'a, Result<bytes::Bytes, reqwest::Error>>,
    delimiter: String,
) -> LocalBoxStream<'a, Result<bytes::Bytes, reqwest::Error>> {
    let stream_buffer = Arc::new(Mutex::new(BytesMut::new()));

    let res = stream.filter_map(move |chunk_result| {
        let stream_buffer = Arc::clone(&stream_buffer);
        let delimiter = delimiter.as_bytes().to_vec();

        async move {
            match chunk_result {
                Ok(ref chunk) => {
                    let chunk_bytes = chunk.as_ref();
                    let mut stream_buffer = stream_buffer.lock().unwrap();

                    match last_delim_index(chunk_bytes, &delimiter) {
                        Some(index) => {
                            let mut i = 0;

                            while i < index {
                                if chunk_bytes[i..].starts_with(&delimiter) {
                                    stream_buffer.extend_from_slice(b"\n");

                                    i = i + delimiter.len();
                                } else {
                                    stream_buffer
                                        .extend_from_slice(&chunk_bytes[i..i + 1]);

                                    i += 1;
                                }
                            }

                            let records = stream_buffer.clone();
                            stream_buffer.clear();

                            stream_buffer.extend_from_slice(&chunk_bytes[i + delimiter.len()..]);

                            Some(Ok(records.freeze()))
                        }
                        None => {
                            stream_buffer.extend_from_slice(chunk_bytes);

                            None
                        }
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
    async fn test_accumulator_stream_concatenates_chunks() {
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
            super::accumulator_stream(boxed, "!".to_string());

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
    async fn test_accumulator_stream_handles_remainders() {
        let inner_stream = futures::stream::iter(vec![
            Ok(bytes::Bytes::from("Hello wo")),
            Ok(bytes::Bytes::from("rld! Wel")),
            Ok(bytes::Bytes::from("come to NY!")),
        ]);
        let boxed = inner_stream.boxed_local();

        let mut chunked_stream =
            super::accumulator_stream(boxed, "!".to_string());

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
    async fn test_accumulator_stream_handles_multi_record_chunks() {
        let inner_stream = futures::stream::iter(vec![Ok(bytes::Bytes::from(
            "Hello world! Welcome to NY! Glad you coul",
        ))]);
        let boxed = inner_stream.boxed_local();

        let mut chunked_stream =
            super::accumulator_stream(boxed, "!".to_string());

        let first_chunk = chunked_stream.next().await;
        assert_eq!(
            first_chunk.unwrap().unwrap(),
            bytes::Bytes::from("Hello world\n Welcome to NY")
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
    fn test_split() {
        let s1 = b"Hello world! Welcome to NY! Glad you coul";

        let split = s1.split(|&c| c == b'!').collect::<Vec<&[u8]>>();
        let first: &[u8] = b"Hello world";
        let second: &[u8] = b" Welcome to NY";
        let third: &[u8] = b" Glad you coul";

        assert_eq!(split, vec![first, second, third]);

        let s2 = b"! Welcome to NY! Glad you coul";

        let split = s2.split(|&c| c == b'!').collect::<Vec<&[u8]>>();
        let first: &[u8] = b"";
        let second: &[u8] = b" Welcome to NY";
        let third: &[u8] = b" Glad you coul";

        assert_eq!(split, vec![first, second, third]);

        let s3 = b"hello world! welcome to ny!";

        let split = s3.split(|&c| c == b'!').collect::<Vec<&[u8]>>();
        let first: &[u8] = b"hello world";
        let second: &[u8] = b" welcome to ny";
        let third: &[u8] = b"";

        assert_eq!(split, vec![first, second, third]);

        let s4 = b"hello world";

        let split = s4.split(|&c| c == b'!').collect::<Vec<&[u8]>>();
        let first: &[u8] = b"hello world";

        assert_eq!(split, vec![first]);
    }
}
