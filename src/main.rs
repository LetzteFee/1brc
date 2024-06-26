#[cfg(test)]
mod tests;

use hashbrown::HashMap;
use std::{
    fs,
    io::{self, Read, Write},
    str,
    sync::{
        mpsc::{self, Sender},
        Arc, Mutex,
    },
    thread, vec,
};

const BUFFER_SIZE: usize = 100_000_000;
const PATH: &str = "1brc/data/measurements.txt";
const N_MAX_THREADS: usize = 8;

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
    buffer_offset: usize,
}
impl BufferManager {
    fn with(file: fs::File) -> BufferManager {
        BufferManager {
            file,
            buffer: Some(vec![0; BUFFER_SIZE]),
            buffer_offset: 0,
        }
    }
    #[inline(always)]
    fn request_buffer(&mut self, mut new_buffer: Vec<u8>) -> Option<Vec<u8>> {
        let mut old_buffer: Vec<u8> = self.buffer.take()?;
        let copied_data_len: usize = self
            .file
            .read(&mut old_buffer[self.buffer_offset..])
            .unwrap();

        if self.buffer_offset + copied_data_len < BUFFER_SIZE {
            old_buffer.truncate(self.buffer_offset + copied_data_len);
            return match old_buffer.is_empty() {
                true => None,
                false => Some(old_buffer),
            };
        }

        for (offset, byte) in old_buffer.iter().rev().enumerate() {
            if *byte == b'\n' {
                self.buffer_offset = offset;
                break;
            }
        }
        // since the buffer is full there must be a linebreak

        if self.buffer_offset > 0 {
            let new_buffer_slice: &mut [u8] = &mut new_buffer[..self.buffer_offset];
            new_buffer_slice
                .copy_from_slice(&old_buffer[(old_buffer.len() - self.buffer_offset)..]);
        }
        old_buffer.truncate(old_buffer.len() - self.buffer_offset - 1);

        self.buffer = Some(new_buffer);
        Some(old_buffer)
    }
}
fn main() -> io::Result<()> {
    let buffer_manager: Arc<Mutex<BufferManager>> =
        Arc::new(Mutex::new(BufferManager::with(fs::File::open(PATH)?)));
    let (map_sender, map_receiver) = mpsc::channel::<HashMap<String, Station>>();

    for _ in 0..N_MAX_THREADS {
        new_thread(buffer_manager.clone(), map_sender.clone());
    }

    let mut map: HashMap<String, Station> = map_receiver.recv().unwrap();
    for _ in 1..N_MAX_THREADS {
        for (name, tmp_station) in map_receiver.recv().unwrap().drain() {
            map.entry(name)
                .and_modify(|station| station.join(&tmp_station))
                .or_insert(tmp_station);
        }
    }
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

fn new_thread(
    buffer_manager: Arc<Mutex<BufferManager>>,
    map_sender: Sender<HashMap<String, Station>>,
) {
    thread::spawn(move || {
        let mut map: HashMap<String, Station> = HashMap::with_capacity(10_000);
        let mut possible_buffer: Option<Vec<u8>> = {
            buffer_manager
                .lock()
                .unwrap()
                .request_buffer(vec![0; BUFFER_SIZE])
        };
        while let Some(mut buffer) = possible_buffer {
            process_buffer(&buffer, &mut map);
            buffer.resize(BUFFER_SIZE, 0);
            possible_buffer = buffer_manager.lock().unwrap().request_buffer(buffer);
        }
        map_sender.send(map).unwrap();
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
