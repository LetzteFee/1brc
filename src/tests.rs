#[test]
fn test_linebreak() {
    use super::find_linebreak;
    let string_slice = "blabla\nbla";
    assert_eq!(6, find_linebreak(string_slice.as_bytes()).unwrap());
}