use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::hash::Hash;
use std::iter::Map;

#[derive(Debug)]
pub enum LruError {
    SmallCacheSize,
}

impl fmt::Display for LruError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LruError::SmallCacheSize => write!(f, "cache size should be greater than 1"),
        }
    }
}

impl Error for LruError { }

#[derive(Debug)]
pub struct LinkedNode<K, V> {
    next: usize,
    prev: usize,
    key: Option<K>,
    value: Option<V>,
}

#[derive(Debug)]
pub struct Lru<K, V> {
    key_location: HashMap<K, usize>,
    use_sequence: Vec<LinkedNode<K, V>>,
    current: usize,
}

impl<K: Eq + Hash + Copy, V> Lru<K, V> {
    pub fn new(max_len: usize) -> Result<Lru<K, V>, LruError> {
        if max_len < 2 { return Err(LruError::SmallCacheSize) }

        let mut use_sequence = vec![];
        for i in 0..max_len {
            let (prev, next) = match i {
                0 => (max_len - 1, 1),
                _last if i == max_len - 1 => (max_len - 2, 0),
                _ => (i - 1, i + 1),
            };
            use_sequence.push(LinkedNode { key: None, value: None, next, prev });
        }


        let key_location = HashMap::new();

        Ok(Lru { use_sequence, key_location, current: 0 })
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        match self.key_location.get(key) {
            Some(&key_index) => {
                self.bump_key(key_index);
                self.use_sequence[key_index].value.as_ref()
            },
            None => None,
        }
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        match self.key_location.get(key) {
            Some(&key_index) => {
                self.bump_key(key_index);
                self.use_sequence[key_index].value.as_mut()
            },
            None => None,
        }
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.key_location.contains_key(key)
    }

    pub fn set(&mut self, key: K, value: V) -> Option<(K, V)> {
        match self.key_location.get(&key) {
            Some(&key_index) => {
                self.use_sequence[key_index].value = Some(value);
                self.bump_key(key_index);
                None
            },
            None => {
                let old_node = &self.use_sequence[self.current];
                match &old_node.key {
                    Some(key) => self.key_location.remove(key),
                    None => None,
                };
                self.key_location.insert(key, self.current);

                let mut value_opt = Some(value);
                let mut key_opt = Some(key);
                std::mem::swap(&mut self.use_sequence[self.current].key, &mut key_opt);
                std::mem::swap(&mut self.use_sequence[self.current].value, &mut value_opt);
                self.increment_current();
                match (key_opt, value_opt) {
                    (Some(k), Some(v)) => Some((k, v)),
                    _ => None,
                }
            },

        }
    }

    fn bump_key(&mut self, key_index: usize) {
        self.skip_key(key_index);
        self.drag_key_before_current(key_index);
    }

    fn skip_key(&mut self, key_index: usize) {
        let prev_index = self.use_sequence[key_index].prev;
        let next_index = self.use_sequence[key_index].next;

        self.use_sequence[prev_index].next = next_index;
        self.use_sequence[next_index].prev = prev_index;
    }

    fn drag_key_before_current(&mut self, key_index: usize) {
        if key_index == self.current {
            self.increment_current();
        } else if self.use_sequence[self.current].prev == key_index {
        } else {
            let recent_index = self.use_sequence[self.current].prev;
            self.use_sequence[recent_index].next = key_index;
            self.use_sequence[self.current].prev = key_index;
            self.use_sequence[key_index].prev = recent_index;
            self.use_sequence[key_index].next = self.current;
        }
    }

    fn increment_current(&mut self) {
        self.current = self.use_sequence[self.current].next;
    }
}

impl<K, V> IntoIterator for Lru<K, V> {
    type Item = Option<(K, V)>;
    type IntoIter = Map<std::vec::IntoIter<LinkedNode<K, V>>, fn(LinkedNode<K, V>) -> Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.use_sequence.into_iter().map(|node| {
            match (node.key, node.value) {
                (Some(key), Some(value)) => Some((key, value)),
                _ => None,
            }
        })
    }
}

impl<K, V> Default for Lru<K, V> {
    fn default() -> Self {
        Self {
            key_location: HashMap::new(),
            use_sequence: vec![],
            current: 0,
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new() {
        assert!(Lru::<i32, i32>::new(0).is_err());
        assert!(Lru::<i32, i32>::new(1).is_err());
        assert!(Lru::<i32, i32>::new(2).is_ok());
        assert!(Lru::<i32, i32>::new(1000).is_ok());
    }


    #[test]
    fn set_get_2() {
        let mut lru = Lru::<i32, i32>::new(2).unwrap();

        assert!(lru.set(1, 1).is_none());
        assert!(lru.set(2, 2).is_none());

        assert_eq!(lru.get(&1), Some(&1));

        assert_eq!(lru.set(3, 3), Some((2, 2)));

        assert_eq!(lru.get(&1), Some(&1));
        assert_eq!(lru.get(&2), None);
        assert_eq!(lru.get(&3), Some(&3));
    }

    #[test]
    fn set_get_3() {
        let mut lru = Lru::<i32, &str>::new(3).unwrap();
        assert!(lru.set(1, "one").is_none());
        assert!(lru.set(2, "two").is_none());

        assert_eq!(lru.get(&1), Some(&"one"));

        assert!(lru.set(3, "three").is_none());

        assert_eq!(lru.get(&3), Some(&"three"));
        assert_eq!(lru.get(&1), Some(&"one"));
        assert_eq!(lru.get(&2), Some(&"two"));

        assert_eq!(lru.set(4, "four"), Some((3, "three")));

        assert_eq!(lru.get(&4), Some(&"four"));
        assert_eq!(lru.get(&3), None);
        assert_eq!(lru.get(&2), Some(&"two"));
        assert_eq!(lru.get(&1), Some(&"one"));

        assert_eq!(lru.set(5, "five"), Some((4, "four")));
        assert_eq!(lru.set(6, "six"), Some((2, "two")));

        assert_eq!(lru.get(&1), Some(&"one"));
        assert_eq!(lru.get(&2), None);
        assert_eq!(lru.get(&3), None);
        assert_eq!(lru.get(&4), None);
        assert_eq!(lru.get(&5), Some(&"five"));
        assert_eq!(lru.get(&6), Some(&"six"));
    }

    #[test]
    fn iterate() {
        let mut lru = Lru::<i32, &str>::new(2).unwrap();
        lru.set(1, "one");
        lru.set(2, "two");
        lru.set(3, "three");
        let contents: Vec<(i32, &str)> = lru.into_iter().map(|el| el.unwrap()).collect();

        assert!(matches!(contents[..], [(3, "three"), (2, "two")]));
    }
}
