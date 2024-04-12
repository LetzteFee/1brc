#[test]
fn test_linebreak() {
    use super::find_linebreak;
    let string_slice = "blabla\nbla";
    assert_eq!(6, find_linebreak(string_slice.as_bytes()).unwrap());
}
#[test]
fn test_station() {
    use super::Station;
    let mut station = Station::from(0.5);
    station.update(-5.1);
    station.update(5.1);
    assert_eq!(String::from("=-5.1/0.2/5.1"), station.drain());
}
