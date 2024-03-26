/*
Strategie:
1. Buffer vollmachen
2. Jede Zeile bearbeiten, bis ausschließlich
    zur letzten Zeile, die wahrscheinlich nicht vollständig ist
3. Den Rest in den Anfang des Buffers reinkopieren
4. Wiederholen
*/
use std::{collections::HashMap, env, fs, io::Read, str};

#[derive(Debug)]
struct Station {
    // each value is 10x of the original value
    min: f64,
    max: f64,
    sum: i128,
    count: u128,
}
impl Station {
    fn new() -> Station {
        Station {
            min: f64::MAX,
            max: f64::MIN,
            sum: 0,
            count: 0,
        }
    }
    fn update(&mut self, value: f64) {
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
        self.sum += (value * 10.0) as i128;
        self.count += 1;
    }
    fn drain(self, station_name: &str) {
        let mean: f64 = (self.sum as f64) / (self.count as f64);
        print!(
            "{}={:.1}/{:.1}/{:.1}",
            station_name, self.min, mean, self.max
        );
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let path: &String = &args[1];

    let file: fs::File = fs::File::open(path).unwrap();
    let mut map: HashMap<String, Station> = HashMap::new();

    process(file, &mut map);

    print!("{{");
    // let mut first = true;
    for (key, value) in map.drain() {
        // if first {
        // println!("{:?}", value);
        // first = false;
        // }
        value.drain(&key);
        print!(",");
    }
    println!("}}");
}
fn process(mut file: fs::File, map: &mut HashMap<String, Station>) {
    let mut buffer: Vec<u8> = vec![0; 20_000_000_000];

    let mut buffer_offset: usize = 0;
    loop {
        // get data
        let copied_data_length: usize = file.read(&mut buffer[buffer_offset..]).unwrap();
        let data_length = copied_data_length + buffer_offset;

        // make data readable
        // last cycle
        if data_length < buffer.len() {
            let string = String::from_utf8_lossy(&buffer[..data_length]);
            for line in string.lines() {
                process_line(line, map);
            }
            break;
        }

        let mut valid_len: Option<usize> = None;
        let string: &str = match str::from_utf8(&buffer) {
            Ok(k_string) => k_string,
            Err(u8r) => {
                valid_len = Some(u8r.valid_up_to());
                str::from_utf8(&buffer[..u8r.valid_up_to()]).unwrap()
            }
        };
        let string_ends_with_new_line: bool = string.ends_with('\n');

        let mut lines: Vec<&str> = string.lines().collect();
        let mut last_line: String = String::from(lines.pop().unwrap());
        if string_ends_with_new_line {
            last_line.push('\n');
        }

        // process data
        for line in lines {
            process_line(line, map);
        }

        // preparations for next cycle
        buffer_offset = 0;
        for (index, byte) in last_line.as_bytes().iter().enumerate() {
            buffer[index] = *byte;
            buffer_offset = index + 1;
        }
        if let Some(v_len) = valid_len {
            let leftover_bytes = Vec::from(&buffer[v_len..]);
            for byte in leftover_bytes {
                buffer[buffer_offset] = byte;
                buffer_offset += 1;
            }
        }
    }
}
fn process_line(line: &str, map: &mut HashMap<String, Station>) {
    let mut line_iter = line.split(';');
    let name: &str = line_iter.next().expect("line should contain something");
    let value_str: &str = line_iter.next().expect("line should contain a semicolon");
    let value: f64 = value_str
        .parse()
        .expect("second part should contain a valid number");

    if !map.contains_key(name) {
        map.insert(String::from(name), Station::new());
    }

    map.get_mut(name).unwrap().update(value);
}
