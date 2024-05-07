#[cfg(test)]
mod tests;

use hashbrown::HashMap;
use std::{
    env, fs,
    io::{self, Read, Write},
    num::NonZeroUsize,
    str,
    sync::{
        mpsc::{self, Sender},
        Arc, Mutex,
    },
    thread, vec,
};

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
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
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
        if tmp_station.min < self.min {
            self.min = tmp_station.min;
        }
        if tmp_station.max > self.max {
            self.max = tmp_station.max;
        }
        self.sum += tmp_station.sum;
        self.count += tmp_station.count;
    }
}

struct BufferManager {
    file: fs::File,
    buffer: Option<Vec<u8>>,
    size: usize,
}
impl BufferManager {
    fn with(file: fs::File, size: usize) -> BufferManager {
        BufferManager {
            file,
            buffer: Some(Vec::with_capacity(size)),
            size,
        }
    }
    #[inline(always)]
    fn request_buffer(&mut self) -> Option<Vec<u8>> {
        let mut old_buffer: Vec<u8> = self.buffer.take()?;
        let old_length: usize = old_buffer.len();
        old_buffer.resize(self.size, 0);
        let copied_data_len: usize = self.file.read(&mut old_buffer[old_length..]).unwrap();
        old_buffer.resize(old_length + copied_data_len, 0);

        if old_buffer.len() == self.size {
            let new_buffer: Vec<u8> =
                old_buffer.split_off(find_last_linebreak(&old_buffer).unwrap() + 1);
            let _ = old_buffer.pop().unwrap();
            self.buffer = Some(new_buffer);
        }
        Some(old_buffer)
    }
}
fn main() -> io::Result<()> {
    let mut args = env::args().skip(1);
    let path = args
        .next()
        .unwrap_or(String::from("1brc/data/measurements.txt"));

    let buffer_size: usize = match args.next() {
        Some(string) => string.parse().unwrap(),
        None => 100_000_000,
    };

    let file: fs::File = fs::File::open(path)?;
    let map = create_map_from_file(file, buffer_size);
    print_map(map);
    Ok(())
}
#[inline(always)]
fn print_map(mut map: HashMap<String, Station>) {
    let mut result: Vec<String> = Vec::new();
    for (name, value) in map.drain() {
        let values: String = value.drain();
        result.push(name + &values);
    }
    result.sort();

    let mut lock = io::stdout().lock();

    let mut iter: vec::IntoIter<String> = result.into_iter();
    write!(lock, "{{ {}", iter.next().unwrap()).unwrap();
    for element in iter {
        write!(lock, ", {}", element).unwrap();
    }
    writeln!(lock, " }}").unwrap();
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
fn create_map_from_file(file: fs::File, buffer_size: usize) -> HashMap<String, Station> {
    let mut map: HashMap<String, Station> = HashMap::with_capacity(0);
    let max_threads: NonZeroUsize =
        thread::available_parallelism().unwrap_or(NonZeroUsize::new(8).unwrap());
    let mut n_current_threads: usize = 0;

    let buffer_manager: Arc<Mutex<BufferManager>> =
        Arc::new(Mutex::new(BufferManager::with(file, buffer_size)));
    let (tx, rx) = mpsc::channel::<HashMap<String, Station>>();

    while n_current_threads < usize::from(max_threads) {
        new_thread(Arc::clone(&buffer_manager), tx.clone());
        n_current_threads += 1;
    }

    loop {
        let tmp_map: HashMap<String, Station> = rx.recv().unwrap();
        n_current_threads -= 1;

        if map.is_empty() {
            map = tmp_map;
        } else {
            update_maps(&mut map, tmp_map);
        }

        if n_current_threads == 0 {
            return map;
        }
    }
}
#[inline(always)]
fn find_last_linebreak(buffer: &[u8]) -> Option<usize> {
    for (index, byte) in buffer.iter().enumerate().rev() {
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
        let mut possible_buffer: Option<Vec<u8>> =
            { buffer_manager.lock().unwrap().request_buffer() };
        while let Some(buffer) = possible_buffer {
            process_buffer(&buffer, &mut map);
            possible_buffer = buffer_manager.lock().unwrap().request_buffer();
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
        let value: f64 = line_iter
            .next()
            .expect("line should contain a semicolon")
            .parse()
            .expect("second part should contain a valid number");

        if map.contains_key(name) {
            map.get_mut(name).unwrap().update(value);
        } else {
            map.insert(String::from(name), Station::from(value));
        }
    }
}
