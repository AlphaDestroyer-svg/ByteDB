use std::collections::{HashMap, VecDeque};
use parking_lot::Mutex;

pub struct LruKReplacer {
    k: usize,
    access_history: Mutex<HashMap<usize, VecDeque<u64>>>,
    current_timestamp: Mutex<u64>,
    evictable: Mutex<HashMap<usize, bool>>,
}

impl LruKReplacer {
    pub fn new(_max_size: usize, k: usize) -> Self {
        LruKReplacer {
            k,
            access_history: Mutex::new(HashMap::new()),
            current_timestamp: Mutex::new(0),
            evictable: Mutex::new(HashMap::new()),
        }
    }

    pub fn record_access(&self, frame_id: usize) {
        let mut ts = self.current_timestamp.lock();
        *ts += 1;
        let timestamp = *ts;
        drop(ts);

        let mut history = self.access_history.lock();
        let entry = history.entry(frame_id).or_insert_with(VecDeque::new);
        entry.push_back(timestamp);
        if entry.len() > self.k {
            entry.pop_front();
        }
    }

    pub fn set_evictable(&self, frame_id: usize, evictable: bool) {
        self.evictable.lock().insert(frame_id, evictable);
    }

    pub fn evict(&self) -> Option<usize> {
        let history = self.access_history.lock();
        let evictable = self.evictable.lock();

        let mut victim: Option<usize> = None;
        let mut max_distance: u64 = 0;
        let mut found_less_than_k = false;

        for (&frame_id, &is_evictable) in evictable.iter() {
            if !is_evictable {
                continue;
            }

            if let Some(accesses) = history.get(&frame_id) {
                if accesses.len() < self.k {
                    if !found_less_than_k {
                        found_less_than_k = true;
                        victim = Some(frame_id);
                        max_distance = accesses.front().copied().unwrap_or(0);
                    } else {
                        let earliest = accesses.front().copied().unwrap_or(0);
                        if earliest < max_distance {
                            max_distance = earliest;
                            victim = Some(frame_id);
                        }
                    }
                } else if !found_less_than_k {
                    let kth_back = accesses.front().copied().unwrap_or(0);
                    let distance = self.current_timestamp.lock().saturating_sub(kth_back);
                    if distance > max_distance {
                        max_distance = distance;
                        victim = Some(frame_id);
                    }
                }
            } else {
                if !found_less_than_k {
                    found_less_than_k = true;
                }
                victim = Some(frame_id);
                max_distance = 0;
            }
        }

        if let Some(frame_id) = victim {
            drop(history);
            drop(evictable);
            self.remove(frame_id);
        }

        victim
    }

    pub fn remove(&self, frame_id: usize) {
        self.access_history.lock().remove(&frame_id);
        self.evictable.lock().remove(&frame_id);
    }

    pub fn size(&self) -> usize {
        self.evictable
            .lock()
            .values()
            .filter(|&&v| v)
            .count()
    }
}
