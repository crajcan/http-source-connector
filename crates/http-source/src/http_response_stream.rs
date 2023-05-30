use bytes::BytesMut;
use futures::{stream::LocalBoxStream, StreamExt};
use std::sync::{Arc, Mutex};

pub fn chunk_http_stream<'a>(
    stream: LocalBoxStream<'a, Result<bytes::Bytes, reqwest::Error>>,
    delimiter: String,
) -> LocalBoxStream<'a, Result<bytes::Bytes, reqwest::Error>> {
    let buffer = Arc::new(Mutex::new(BytesMut::new()));

    let res = stream.filter_map(move |chunk_result| {
        let buffer = Arc::clone(&buffer);
        let delimiter = delimiter.as_bytes().to_vec();

        async move {
            match chunk_result {
                Ok(ref chunk) => {
                    let chunk_bytes = chunk.as_ref();

                    let mut buffer = buffer.lock().unwrap();

                    buffer.extend_from_slice(chunk_bytes);

                    if chunk_bytes.ends_with(&delimiter) {
                        let record = buffer.clone();
                        buffer.clear();

                        Some(Ok(record.freeze()))
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

#[cfg(test)]
mod test {
    use futures::{stream::LocalBoxStream, StreamExt};
    use std::boxed;

    #[async_std::test]
    async fn test_chunk_http_stream_concatenates_chunks() {
        let inner_stream = futures::stream::iter(vec![
            Ok(bytes::Bytes::from("hello")),
            Ok(bytes::Bytes::from(" world")),
            Ok(bytes::Bytes::from("!")),
            Ok(bytes::Bytes::from("Welcome")),
            Ok(bytes::Bytes::from(" to ")),
            Ok(bytes::Bytes::from("NY!")),
        ]);
        let boxed = inner_stream.boxed_local();

        let mut chunked_stream =
            super::chunk_http_stream(boxed, "!".to_string());

        let first_chunk = chunked_stream.next().await;
        assert_eq!(
            first_chunk.unwrap().unwrap(),
            bytes::Bytes::from("hello world!")
        );

        let second_chunk = chunked_stream.next().await;
        assert_eq!(
            second_chunk.unwrap().unwrap(),
            bytes::Bytes::from("Welcome to NY!")
        );
    }
}
