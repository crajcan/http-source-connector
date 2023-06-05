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
                    // }