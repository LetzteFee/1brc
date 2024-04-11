#[cfg(test)]
mod tests;

use hashbrown::HashMap;
use std::{
    env, fs,
    io::Read,
    num::NonZeroUsize,
    str,
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
    #[inline(always)]
    fn from(value: f64) -> Station {
        Station {
            min: value,
            max: value,
            sum: (value * 10.0) as i128,
            count: 1,
        }
    }
    #[inline(always)]
    fn update(&mut self, value: f64) {
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.sum += (value * 10.0) as i128;
        self.count += 1;
    }
    #[inline(always)]
    fn drain(self) -> String {
        let correct_sum: f64 = (self.sum as f64) / 10.0;
        let mean: f64 = correct_sum / (self.count as f64);
        format!("={}/{:.1}/{}", self.min, mean, self.max)
    }
    #[inline(always)]
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
    n_threads: NonZeroUsize,
    n_served_buffers: usize,
}
impl BufferManager {
    fn with(size: usize, mut file: fs::File, n_threads: NonZeroUsize) -> BufferManager {
        let mut buffers: Vec<DebtVec> = vec![DebtVec::with(size)];

        let copied_data_len = file.read(buffers[0].slice_mut()).unwrap();
        buffers[0].vec.resize(copied_data_len, 0);

        BufferManager {
            file,
            buffers,
            size,
            n_threads,
            n_served_buffers: 0,
        }
    }
    fn get_dynamic_size(&mut self) -> usize {
        if self.n_served_buffers + 1 >= usize::from(self.n_threads) {
            (self.size as f32 * 1.5) as usize
        } else {
            self.size
        }
    }

    #[inline(always)]
    fn request_buffer(&mut self) -> Option<DebtVec> {
        if self.buffers.is_empty() {
            return None;
        }

        let mut tmp_buffer: DebtVec = DebtVec::with(self.get_dynamic_size());
        let copied_data_len: usize = self.file.read(tmp_buffer.slice_mut()).unwrap();
        // at this point there is no offset
        tmp_buffer.vec.resize(copied_data_len, 0);
        self.n_served_buffers += 1;
        match find_linebreak(tmp_buffer.slice()) {
            Some(index_linebreak) => {
                self.buffers[0].extend(tmp_buffer.pop_front(index_linebreak));
                let _linebreak: &[u8] = tmp_buffer.pop_front(1);
                let output = self.buffers.pop().unwrap();
                self.buffers.push(tmp_buffer);
                Some(output)
            }
            None => {
                // buffer_a likely has an offset but we only change the end of the vector;
                // it copies but is does not matter because they're just bytes
                self.buffers[0].vec.extend_from_slice(tmp_buffer.slice());
                Some(self.buffers.pop().unwrap())
            }
        }
    }
}
fn main() {
    let mut args = env::args().skip(1);
    let path = args
        .next()
        .unwrap_or(String::from("1brc/data/measurements.txt"));

    let file: fs::File = fs::File::open(path).unwrap();

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
#[inline]
fn update_maps(main_map: &mut HashMap<String, Station>, mut tmp_map: HashMap<String, Station>) {
    for (name, tmp_station) in tmp_map.drain() {
        main_map
            .entry(name)
            .and_modify(|station| station.join(&tmp_station))
            .or_insert(tmp_station);
    }
}
fn create_map_from_file(file: fs::File) -> HashMap<String, Station> {
    let mut map: HashMap<String, Station> = HashMap::with_capacity(0);
    let max_threads: NonZeroUsize =
        thread::available_parallelism().unwrap_or(NonZeroUsize::new(8).unwrap());
    let mut n_current_threads: usize = 0;

    let buffer_manager: Arc<Mutex<BufferManager>> = Arc::new(Mutex::new(BufferManager::with(
        40_000_000,
        file,
        max_threads,
    )));
    let (tx, rx) = mpsc::channel::<HashMap<String, Station>>();

    while n_current_threads < usize::from(max_threads) {
        new_thread(Arc::clone(&buffer_manager), tx.clone());
        n_current_threads += 1;
    }

    while let Ok(tmp_map) = rx.recv() {
        n_current_threads -= 1;

        if map.is_empty() {
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
#[inline(always)]
fn find_linebreak(buffer: &[u8]) -> Option<usize> {
    for (index, byte) in buffer.iter().enumerate() {
        // TODO: Is this how UTF-8 works?
        if *byte == b'\n' {
            return Some(index);
        }
    }
    None
}
#[inline(always)]
fn new_thread(buffer_manager: Arc<Mutex<BufferManager>>, tx: Sender<HashMap<String, Station>>) {
    thread::spawn(move || {
        let mut map: HashMap<String, Station> = HashMap::with_capacity(10_000);
        loop {
            let possible_buffer: Option<DebtVec> =
                { buffer_manager.lock().unwrap().request_buffer() };
            match possible_buffer {
                Some(buffer) => process_buffer(buffer.slice(), &mut map),
                None => break,
            };
        }
        tx.send(map).unwrap();
    });
}
#[inline(always)]
fn process_buffer(buffer: &[u8], map: &mut HashMap<String, Station>) {
    let string_slice = unsafe { str::from_utf8_unchecked(buffer) };
    for line in string_slice.lines() {
        let mut line_iter = line.split(';');
        let name: &str = line_iter.next().expect("line should contain something");
        let value_str: &str = line_iter.next().unwrap();
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
