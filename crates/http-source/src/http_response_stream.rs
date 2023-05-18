use futures::stream::LocalBoxStream;
// use futures_util::stream::Stream;
use bytes::{Bytes, BytesMut};
use futures::stream::Stream;
use pin_utils::unsafe_pinned;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

// use bytes::BytesMut;

pub struct HttpResponseStream<'a> {
    inner: LocalBoxStream<'a, Result<bytes::Bytes, reqwest::Error>>,
    buffer: BytesMut,
    delimiter: Vec<u8>,
}

impl<'a> HttpResponseStream<'a> {
    unsafe_pinned!(
        inner: LocalBoxStream<'a, Result<bytes::Bytes, reqwest::Error>>
    );

    pub fn new(
        stream: LocalBoxStream<'a, Result<bytes::Bytes, reqwest::Error>>,
        delimiter: Vec<u8>,
    ) -> Self {
        let buffer = BytesMut::new();

        HttpResponseStream {
            inner: stream,
            buffer,
            delimiter,
        }
    }
}

impl<'a> Stream for HttpResponseStream<'a> {
    type Item = Result<Bytes, reqwest::Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let inner_state = self.as_mut().inner.as_mut().poll_next(cx);

        if let Poll::Ready(Some(Ok(ref chunk))) = &inner_state {
            self.as_mut().buffer.extend_from_slice(chunk);

            if self.buffer.ends_with(&self.delimiter) {
                println!("chunk ends with delimiter: {:?}", chunk);

                let mut res = BytesMut::new();
                std::mem::swap(&mut self.buffer, &mut res);

                println!(
                    "    http_response_stream returning buffer: {:?}",
                    res
                );

                return Poll::Ready(Some(Ok(res.into())));
            }
        }

        inner_state
    }
}

// impl<'a> Stream for HttpResponseStream<'a> {
//     type Item = Result<Bytes, reqwest::Error>;

//     fn poll_next(
//         mut self: Pin<&mut Self>,
//         cx: &mut Context<'_>,
//     ) -> Poll<Option<Self::Item>> {
//         println!("in poll_next for HttpResponseStream");

//         let inner_result = self.as_mut().inner.as_mut().poll_next(cx);
//         match inner_result {
//             Poll::Pending => {
//                 println!("Poll::Pending in HttpResponseStream::poll_next");
//                 inner_result
//             }
//             Poll::Ready(None) => {
//                 println!("Poll::Ready(None) in HttpResponseStream::poll_next");
//                 Poll::Ready(None)
//             }
//             Poll::Ready(Some(chunk)) => {
//                 println!(
//                     "received a chunk in HttpResponseStream::poll_next: {:?}",
//                     chunk
//                 );
//                 self.as_mut().buffer.extend_from_slice(&chunk?);

//                 if self.buffer.ends_with(&self.delimiter) {
//                     println!("chunk ends with delimiter");

//                     let mut res = BytesMut::new();
//                     std::mem::swap(&mut self.buffer, &mut res);

//                     println!(
//                         "http_response_stream returning buffer: {:?}",
//                         res
//                     );
//                     Poll::Ready(Some(Ok(res.into())))
//                 } else {
//                     println!("chunk does not end with delimiter");
//                     inner_result
//                 }
//             }
//         }
//     }
// }
