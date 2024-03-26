/*
Strategie:
1. Buffer A erstellen und vollmachen
2. Einen nächsten Buffer B erstellen
3. Die erste Zeile in Buffer B finden
3. Diese Zeile zum vorherigen Buffer A geben
4. Buffer A einem neuen Thread geben, der den Buffer bearbeitet
5. Buffer B zu Buffer A umbenennen
6. zu Schritt 2

Verbesserungen:
IEEE runden
*/
use std::{
    collections::HashMap,
    env, fs,
    io::Read,
    num, str,
    sync::mpsc::{self, Sender},
    thread, vec,
};

// A type of vector that allows for .pop_front() in O(1)
// by just remembering to ignore the first elements
struct DebtVec {
    vec: Vec<u8>,
    offset: usize,
}
impl DebtVec {
    fn with(size: usize) -> DebtVec {
        DebtVec {
            vec: vec![0; size],
            offset: 0,
        }
    }
    fn slice_mut(&mut self) -> &mut [u8] {
        &mut self.vec[self.offset..]
    }
    fn slice(&self) -> &[u8] {
        &self.vec[self.offset..]
    }
    fn is_empty(&self) -> bool {
        self.vec.len() <= self.offset
    }
    fn pop_front(&mut self, size: usize) -> &[u8] {
        let old_offset: usize = self.offset;
        self.offset += size;
        &self.vec[old_offset..self.offset]
    }
    fn reset(&mut self) {
        self.vec.clear();
        self.offset = 0;
    }
    /*fn len(&self) -> usize {
        self.vec.len() - self.offset
    }*/
    fn extend(&mut self, slice: &[u8]) {
        self.vec.extend_from_slice(slice);
    }
}

#[derive(Debug)]
struct Station {
    min: f64,
    max: f64,
    sum: i128,
    count: u128,
}
impl Station {
    /*fn new() -> Station {
        Station {
            min: f64::MAX,
            max: f64::MIN,
            sum: 0,
            count: 0,
        }
    }*/
    fn from(value: f64) -> Station {
        Station {
            min: value,
            max: value,
            sum: (value * 10.0) as i128,
            count: 1,
        }
    }
    fn update(&mut self, value: f64) {
        if value < self.min {
            // println!("{} is smaller than {}", value, self.min);
            self.min = value;
        }
        if value > self.max {
            // println!("{} is bigger than {}", value, self.max);
            self.max = value;
        }
        self.sum += (value * 10.0) as i128;
        self.count += 1;
    }
    fn drain(self) -> String {
        let mean: f64 = (self.sum as f64) / (self.count as f64);
        format!("={:.1}/{:.1}/{:.1}", self.min, mean, self.max)
    }
    fn join(&mut self, tmp_station: &Station) {
        self.min = self.min.min(tmp_station.min);
        self.max = self.max.max(tmp_station.max);
        self.sum += tmp_station.sum;
        self.count += tmp_station.count;
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let path: &String = &args[1];

    let file: fs::File = fs::File::open(path).unwrap();
    let mut map: HashMap<String, Station> = HashMap::new();

    thread_manager(file, &mut map);

    let mut result: Vec<String> = Vec::new();
    for (name, value) in map.drain() {
        let values: String = value.drain();
        result.push(name + &values);
    }
    result.sort();

    let mut iter: vec::IntoIter<String> = result.into_iter();
    print!("{{ {}", iter.next().unwrap());
    for element in iter {
        print!(", {}", element);
    }
    println!(" }}");
}
fn update_maps(main_map: &mut HashMap<String, Station>, mut tmp_map: HashMap<String, Station>) {
    for (name, tmp_station) in tmp_map.drain() {
        main_map
            .entry(name)
            .and_modify(|station| station.join(&tmp_station))
            .or_insert(tmp_station);
    }
}
fn thread_manager(mut file: fs::File, map: &mut HashMap<String, Station>) {
    let max_threads: num::NonZeroUsize = match thread::available_parallelism() {
        Ok(threads) => {
            // println!("Number of threads: {}", threads);
            threads
        }
        Err(_) => num::NonZeroUsize::new(8).unwrap(),
    };
    let mut n_current_threads: usize = 0;
    // let mut thread_enumeration: usize = 0;

    let buffer_size: usize = 8_000_000_000 / max_threads;

    let mut buffer_a: DebtVec = DebtVec::with(buffer_size);
    let (tx, rx) = mpsc::channel::<HashMap<String, Station>>();

    if file.read(buffer_a.slice_mut()).unwrap() < buffer_size {
        panic!("buffer_size is bigger than entire file");
    }

    {
        let mut dynamic_buffer_size: usize = buffer_size / 2;
        while n_current_threads < usize::from(max_threads) {
            dynamic_buffer_size += buffer_size / 16;
            buffer_a = new_cycle(
                &mut file,
                buffer_a,
                DebtVec::with(dynamic_buffer_size),
                tx.clone(),
                // thread_enumeration,
            );
            // thread_enumeration += 1;
            n_current_threads += 1;
            if buffer_a.is_empty() {
                break;
            }
        }
    }

    let mut map_buffer: Vec<HashMap<String, Station>> = Vec::new();

    while let Ok(tmp_map) = rx.recv() {
        n_current_threads -= 1;

        if !buffer_a.is_empty() {
            buffer_a = new_cycle(
                &mut file,
                buffer_a,
                DebtVec::with(buffer_size),
                tx.clone(),
                // thread_enumeration,
            );
            // thread_enumeration += 1;
            n_current_threads += 1;
        }

        map_buffer.push(tmp_map);

        if n_current_threads < usize::from(max_threads) {
            while let Some(tmp_map) = map_buffer.pop() {
                update_maps(map, tmp_map);
            }
            if n_current_threads == 0 {
                break;
            }
        }
    }

    assert!(map_buffer.is_empty());
    assert_eq!(n_current_threads, 0);
}

#[inline]
fn new_cycle(
    file: &mut fs::File,
    mut buffer_a: DebtVec,
    mut buffer_b: DebtVec,
    tx: Sender<HashMap<String, Station>>,
    // thread_enumeration: usize,
) -> DebtVec {
    let copied_data_len: usize = file.read(buffer_b.slice_mut()).unwrap();
    // at this point there is no offset
    buffer_b.vec.resize(copied_data_len, 0);

    match find_linebreak(buffer_b.slice()) {
        Some(index_linebreak) => {
            buffer_a.extend(buffer_b.pop_front(index_linebreak));
            let linebreak: u8 = buffer_b.pop_front(1)[0];
            assert_eq!(b'\n', linebreak);
        }
        None => {
            // buffer_a likely has an offset but we only change the end of the vector;
            // it copies but is does not matter because they're just bytes
            buffer_a.vec.extend_from_slice(buffer_b.slice());
            buffer_b.reset();
        }
    }

    /*
    if copied_data_len < 256 {
        // buffer_a likely has an offset but we only change the end of the vector;
        // it copies but is does not matter because they're just bytes
        buffer_a.vec.extend_from_slice(buffer_b.slice());
        buffer_b.reset();
    } else {
        let index_linebreak: usize = find_linebreak(buffer_b.slice()).unwrap();
        // linebreak does not get copied because size is not index
        buffer_a.extend(buffer_b.pop_front(index_linebreak));
        let linebreak: u8 = buffer_b.pop_front(1)[0];
        assert_eq!(b'\n', linebreak);
    }
    */

    thread::spawn(move || {
        // println!("Thread Nr. {} spawned", thread_enumeration);
        let mut map: HashMap<String, Station> = HashMap::new();
        for line in str::from_utf8(buffer_a.slice()).unwrap().lines() {
            process_line(line, &mut map);
        }
        // println!("Thread Nr. {} finished", thread_enumeration);
        tx.send(map).unwrap();
        // println!("Thread Nr. {} received", thread_enumeration);
    });

    buffer_b
}

fn find_linebreak(buffer: &[u8]) -> Option<usize> {
    /*
    let string_slice: &str = match str::from_utf8(buffer) {
        Ok(ok_string) => ok_string,
        Err(u8r) => str::from_utf8(&buffer[..u8r.valid_up_to()]).unwrap()
    };*/
    // string_slice.find('\n').expect("slice should be long enough to contain a line break")
    for (index, byte) in buffer.iter().enumerate() {
        if *byte == b'\n' {
            return Some(index);
        }
    }
    None
}
fn process_line(line: &str, map: &mut HashMap<String, Station>) {
    let mut line_iter = line.split(';');
    let name: &str = line_iter.next().expect("line should contain something");
    let value_str: &str = line_iter.next().expect(line);
    let value: f64 = value_str
        .parse()
        .expect("second part should contain a valid number");

    if !map.contains_key(name) {
        map.insert(String::from(name), Station::from(value));
    } else {
        map.get_mut(name).unwrap().update(value);
    }
}
