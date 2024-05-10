#[test]
fn test_station() {
    use super::Station;
    let mut station = Station::from(0.5);
    station.update(-5.1);
    station.update(5.1);
    assert_eq!(String::from("=-5.1/0.2/5.1"), station.drain());
}
