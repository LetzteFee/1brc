use std::{
    collections::HashMap,
    env, fs,
    io::Read,
    num, str,
    sync::{
        mpsc::{self, Sender},
        Arc, Mutex,
    },
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
    fn pop_front(&mut self, size: usize) -> &[u8] {
        let old_offset: usize = self.offset;
        self.offset += size;
        &self.vec[old_offset..self.offset]
    }
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
    fn from(value: f64) -> Station {
        Station {
            min: value,
            max: value,
            sum: (value * 10.0) as i128,
            count: 1,
        }
    }
    fn update(&mut self, value: f64) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
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

struct BufferManager {
    file: fs::File,
    buffers: Vec<DebtVec>,
    size: usize,
}
impl BufferManager {
    fn width(size: usize, mut file: fs::File) -> BufferManager {
        let mut buffers: Vec<DebtVec> = Vec::new();
        buffers.push(DebtVec::with(size));

        if file.read(buffers[0].slice_mut()).unwrap() < size {
            panic!("Buffer size is bigger than entire file");
        }

        BufferManager {
            file,
            buffers,
            size,
        }
    }
    fn request_buffer(&mut self) -> Option<DebtVec> {
        if self.buffers.len() == 0 {
            return None;
        }

        let mut tmp_buffer: DebtVec = DebtVec::with(self.size);
        let copied_data_len: usize = self.file.read(tmp_buffer.slice_mut()).unwrap();
        // at this point there is no offset
        tmp_buffer.vec.resize(copied_data_len, 0);

        match find_linebreak(tmp_buffer.slice()) {
            Some(index_linebreak) => {
                self.buffers[0].extend(tmp_buffer.pop_front(index_linebreak));
                let _ = tmp_buffer.pop_front(1)[0];
                self.buffers.push(tmp_buffer);
            }
            None => {
                // buffer_a likely has an offset but we only change the end of the vector;
                // it copies but is does not matter because they're just bytes
                self.buffers[0].vec.extend_from_slice(tmp_buffer.slice());
            }
        }

        Some(self.buffers.remove(0))
    }
}
fn main() {
    let mut args = env::args().skip(1);
    let path = args
        .next()
        .unwrap_or(String::from("1brc/data/measurements.txt"));

    let file: fs::File = fs::File::open(&path).unwrap();

    process_map(create_map_from_file(file));
}
fn process_map(mut map: HashMap<String, Station>) {
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
fn create_map_from_file(file: fs::File) -> HashMap<String, Station> {
    let mut map: HashMap<String, Station> = HashMap::new();
    let max_threads: num::NonZeroUsize = match thread::available_parallelism() {
        Ok(threads) => {
            // println!("Number of threads: {}", threads);
            threads
        }
        Err(_) => num::NonZeroUsize::new(8).unwrap(),
    };
    let mut n_current_threads: usize = 0;

    let buffer = Arc::new(Mutex::new(BufferManager::width(
        8_000_000_000 / max_threads,
        file,
    )));
    let (tx, rx) = mpsc::channel::<HashMap<String, Station>>();

    while n_current_threads < usize::from(max_threads) {
        let c_tx: Sender<HashMap<String, Station>> = tx.clone();
        let c_buffer: Arc<Mutex<BufferManager>> = Arc::clone(&buffer);
        thread::spawn(move || {
            let mut t_map: HashMap<String, Station> = HashMap::with_capacity(10_000);
            loop {
                let thread_buffer: Option<DebtVec> = { c_buffer.lock().unwrap().request_buffer() };
                match thread_buffer {
                    Some(data) => process_valid_buffer(data.slice(), &mut t_map),
                    None => break,
                };
            }
            c_tx.send(t_map).unwrap();
        });

        n_current_threads += 1;
    }

    while let Ok(tmp_map) = rx.recv() {
        n_current_threads -= 1;

        if map.len() == 0 {
            map = tmp_map;
        } else {
            update_maps(&mut map, tmp_map);
        }

        if n_current_threads == 0 {
            break;
        }
    }

    assert_eq!(n_current_threads, 0);
    map
}

fn find_linebreak(buffer: &[u8]) -> Option<usize> {
    for (index, byte) in buffer.iter().enumerate() {
        // TODO: Is this how UTF-8 works?
        if *byte == b'\n' {
            return Some(index);
        }
    }
    None
}
fn process_valid_buffer(buffer: &[u8], map: &mut HashMap<String, Station>) {
    let string_slice = str::from_utf8(buffer).unwrap();
    for line in string_slice.lines() {
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
}
