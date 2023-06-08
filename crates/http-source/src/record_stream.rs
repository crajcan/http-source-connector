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

#[cfg(test)]
mod test {
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
