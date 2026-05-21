//! Slotted page format (v0.2).
//!
//! Each 8 KB page is laid out as:
//!
//! ```text
//! +--------------------------------------------------+ 0
//! | header (32 bytes)                                |
//! +--------------------------------------------------+ 32
//! | slot directory, growing forward ->               |
//! |                                                  |
//! |                  free space                      |
//! |                                                  |
//! |                <- tuple data, growing back       |
//! +--------------------------------------------------+ PAGE_SIZE
//! ```
//!
//! The slot directory is an array of fixed 5-byte `SlotEntry { offset: u16,
//! length: u16, flags: u8 }` records, indexed by slot id. Live slots point
//! at their tuple in the data region; dead slots have `flags & SLOT_DEAD
//! != 0` and are reclaimed by `compact()`.
//!
//! Slot ids are stable for the life of the page: deleting a slot leaves a
//! tombstone behind so existing pointers keep resolving (returning `None`),
//! which matches what indexes and MVCC expect.

use std::fmt;

use crate::error::{CoreError, Result};

pub const PAGE_SIZE: usize = 8192;
pub type PageId = u32;
pub type SlotId = u16;

pub const INVALID_PAGE_ID: PageId = 0;

/// Magic stamped into the meta page so v0.1 files refuse to mount.
pub const PAGE_MAGIC_V2: [u8; 4] = *b"BSDB";
pub const PAGE_FORMAT_VERSION: u16 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    Free = 0,
    Data = 1,
    BTreeInternal = 2,
    BTreeLeaf = 3,
    Overflow = 4,
    FreeList = 5,
    Meta = 6,
}

impl From<u8> for PageType {
    fn from(v: u8) -> Self {
        match v {
            0 => PageType::Free,
            1 => PageType::Data,
            2 => PageType::BTreeInternal,
            3 => PageType::BTreeLeaf,
            4 => PageType::Overflow,
            5 => PageType::FreeList,
            6 => PageType::Meta,
            _ => PageType::Free,
        }
    }
}

// Header layout (32 bytes total):
//   [0..4]   page_id           u32 LE
//   [4]      page_type         u8
//   [5]      format_version    u8
//   [6..8]   flags             u16 LE  (reserved)
//   [8..16]  lsn               u64 LE
//   [16..20] checksum          u32 LE  (over body, set on flush)
//   [20..22] slot_count        u16 LE  (incl. dead slots)
//   [22..24] live_count        u16 LE
//   [24..26] free_start        u16 LE  (low end of free space, grows up)
//   [26..28] free_end          u16 LE  (high end of free space, grows down)
//   [28..32] reserved
pub const PAGE_HEADER_SIZE: usize = 32;
pub const SLOT_ENTRY_SIZE: usize = 5;

pub const SLOT_FLAG_DEAD: u8 = 0b0000_0001;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotEntry {
    pub offset: u16,
    pub length: u16,
    pub flags: u8,
}

impl SlotEntry {
    fn encode(&self) -> [u8; SLOT_ENTRY_SIZE] {
        let mut buf = [0u8; SLOT_ENTRY_SIZE];
        buf[0..2].copy_from_slice(&self.offset.to_le_bytes());
        buf[2..4].copy_from_slice(&self.length.to_le_bytes());
        buf[4] = self.flags;
        buf
    }

    fn decode(buf: &[u8]) -> Self {
        SlotEntry {
            offset: u16::from_le_bytes([buf[0], buf[1]]),
            length: u16::from_le_bytes([buf[2], buf[3]]),
            flags: buf[4],
        }
    }

    pub fn is_dead(&self) -> bool {
        self.flags & SLOT_FLAG_DEAD != 0
    }
}

#[derive(Clone)]
pub struct Page {
    pub id: PageId,
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    /// New empty page with header initialised for slotted-page use.
    pub fn new(id: PageId) -> Self {
        let mut page = Page {
            id,
            data: [0u8; PAGE_SIZE],
        };
        page.set_page_id(id);
        page.data[5] = PAGE_FORMAT_VERSION as u8;
        page.set_slot_count(0);
        page.set_live_count(0);
        page.set_free_start(PAGE_HEADER_SIZE as u16);
        page.set_free_end(PAGE_SIZE as u16);
        page
    }

    // ---- header accessors ----

    pub fn page_type(&self) -> PageType {
        PageType::from(self.data[4])
    }

    pub fn set_page_type(&mut self, pt: PageType) {
        self.data[4] = pt as u8;
    }

    pub fn format_version(&self) -> u8 {
        self.data[5]
    }

    pub fn set_page_id(&mut self, id: PageId) {
        self.data[0..4].copy_from_slice(&id.to_le_bytes());
        self.id = id;
    }

    pub fn get_page_id(&self) -> PageId {
        u32::from_le_bytes([self.data[0], self.data[1], self.data[2], self.data[3]])
    }

    pub fn lsn(&self) -> u64 {
        u64::from_le_bytes(self.data[8..16].try_into().unwrap())
    }

    pub fn set_lsn(&mut self, lsn: u64) {
        self.data[8..16].copy_from_slice(&lsn.to_le_bytes());
    }

    pub fn checksum(&self) -> u32 {
        u32::from_le_bytes(self.data[16..20].try_into().unwrap())
    }

    pub fn set_checksum(&mut self, checksum: u32) {
        self.data[16..20].copy_from_slice(&checksum.to_le_bytes());
    }

    pub fn compute_checksum(&self) -> u32 {
        // Checksum covers everything past the checksum field itself.
        crc32fast::hash(&self.data[20..])
    }

    pub fn slot_count(&self) -> u16 {
        u16::from_le_bytes([self.data[20], self.data[21]])
    }

    fn set_slot_count(&mut self, count: u16) {
        self.data[20..22].copy_from_slice(&count.to_le_bytes());
    }

    pub fn live_count(&self) -> u16 {
        u16::from_le_bytes([self.data[22], self.data[23]])
    }

    fn set_live_count(&mut self, count: u16) {
        self.data[22..24].copy_from_slice(&count.to_le_bytes());
    }

    pub fn free_start(&self) -> u16 {
        u16::from_le_bytes([self.data[24], self.data[25]])
    }

    fn set_free_start(&mut self, v: u16) {
        self.data[24..26].copy_from_slice(&v.to_le_bytes());
    }

    pub fn free_end(&self) -> u16 {
        u16::from_le_bytes([self.data[26], self.data[27]])
    }

    fn set_free_end(&mut self, v: u16) {
        self.data[26..28].copy_from_slice(&v.to_le_bytes());
    }

    /// Bytes available for a *new* slot+tuple (must fit both data and entry).
    pub fn free_space(&self) -> usize {
        self.free_end().saturating_sub(self.free_start()) as usize
    }

    // ---- back-compat alias used by older callers ----

    /// Number of *live* tuples on the page. Dead slots are excluded.
    pub fn item_count(&self) -> u16 {
        self.live_count()
    }

    // ---- slot directory ----

    fn slot_entry_offset(&self, slot_id: SlotId) -> usize {
        // Slot directory sits right after the header and grows forward.
        // Slot 0 lives at PAGE_HEADER_SIZE.
        PAGE_HEADER_SIZE + slot_id as usize * SLOT_ENTRY_SIZE
    }

    fn read_slot(&self, slot_id: SlotId) -> SlotEntry {
        let off = self.slot_entry_offset(slot_id);
        SlotEntry::decode(&self.data[off..off + SLOT_ENTRY_SIZE])
    }

    fn write_slot(&mut self, slot_id: SlotId, entry: SlotEntry) {
        let off = self.slot_entry_offset(slot_id);
        let buf = entry.encode();
        self.data[off..off + SLOT_ENTRY_SIZE].copy_from_slice(&buf);
    }

    /// Get the tuple bytes for a slot, or `None` if the slot is dead or OOB.
    pub fn get_slot(&self, slot_id: SlotId) -> Option<&[u8]> {
        if slot_id >= self.slot_count() {
            return None;
        }
        let entry = self.read_slot(slot_id);
        if entry.is_dead() {
            return None;
        }
        let start = entry.offset as usize;
        let end = start + entry.length as usize;
        if end > PAGE_SIZE {
            return None;
        }
        Some(&self.data[start..end])
    }

    /// Allocate a new slot containing `data`. Returns the slot id, or an
    /// error if the page does not have enough free space (caller's job to
    /// split / move to overflow page).
    pub fn alloc_slot(&mut self, data: &[u8]) -> Result<SlotId> {
        if data.len() > u16::MAX as usize {
            return Err(CoreError::Internal(
                "alloc_slot: tuple too large for page".into(),
            ));
        }
        let need = data.len() + SLOT_ENTRY_SIZE;
        if need > self.free_space() {
            return Err(CoreError::Internal(
                "alloc_slot: not enough free space (caller should split)".into(),
            ));
        }

        let slot_id = self.slot_count();
        // Tuple sits just above the previous free_end (tuples grow from the
        // high end of the page downward).
        let tuple_off = self.free_end() - data.len() as u16;
        let tuple_off_usize = tuple_off as usize;
        self.data[tuple_off_usize..tuple_off_usize + data.len()].copy_from_slice(data);

        // Bump slot_count BEFORE writing the entry so slot_entry_offset
        // resolves to the freshly-reserved slot.
        self.set_slot_count(slot_id + 1);
        self.write_slot(
            slot_id,
            SlotEntry {
                offset: tuple_off,
                length: data.len() as u16,
                flags: 0,
            },
        );

        // Slot directory boundary moves up; tuple boundary moves down.
        self.set_free_start(self.free_start() + SLOT_ENTRY_SIZE as u16);
        self.set_free_end(tuple_off);
        self.set_live_count(self.live_count() + 1);
        Ok(slot_id)
    }

    /// Mark a slot as dead. Idempotent. Space is reclaimed by `compact()`.
    pub fn delete_slot(&mut self, slot_id: SlotId) -> Result<()> {
        if slot_id >= self.slot_count() {
            return Err(CoreError::Internal(format!(
                "delete_slot: slot {} out of range (count={})",
                slot_id,
                self.slot_count()
            )));
        }
        let mut entry = self.read_slot(slot_id);
        if entry.is_dead() {
            return Ok(());
        }
        entry.flags |= SLOT_FLAG_DEAD;
        self.write_slot(slot_id, entry);
        let live = self.live_count();
        if live > 0 {
            self.set_live_count(live - 1);
        }
        Ok(())
    }

    /// Replace the contents of a live slot. If the new data is larger than
    /// the existing slot's length and there isn't enough trailing free
    /// space, the slot is rewritten at the end of the data region (and the
    /// old space becomes dead, reclaimable on the next `compact()`).
    pub fn update_slot(&mut self, slot_id: SlotId, data: &[u8]) -> Result<()> {
        if slot_id >= self.slot_count() {
            return Err(CoreError::Internal(format!(
                "update_slot: slot {} out of range",
                slot_id
            )));
        }
        let entry = self.read_slot(slot_id);
        if entry.is_dead() {
            return Err(CoreError::Internal("update_slot: slot is dead".into()));
        }

        if data.len() <= entry.length as usize {
            // In-place overwrite.
            let start = entry.offset as usize;
            self.data[start..start + data.len()].copy_from_slice(data);
            self.write_slot(
                slot_id,
                SlotEntry {
                    offset: entry.offset,
                    length: data.len() as u16,
                    flags: entry.flags,
                },
            );
            return Ok(());
        }

        // Need a fresh region — allocate from free space, then redirect.
        if data.len() + 0 > self.free_space() {
            // No room without compaction; try compacting first.
            self.compact();
            if data.len() > self.free_space() {
                return Err(CoreError::Internal(
                    "update_slot: not enough room even after compact".into(),
                ));
            }
        }
        let new_off = self.free_end() - data.len() as u16;
        let dst = new_off as usize;
        self.data[dst..dst + data.len()].copy_from_slice(data);
        self.set_free_end(new_off);
        self.write_slot(
            slot_id,
            SlotEntry {
                offset: new_off,
                length: data.len() as u16,
                flags: entry.flags,
            },
        );
        Ok(())
    }

    /// Iterate live slots, yielding `(SlotId, &[u8])`.
    pub fn iter_live<'a>(&'a self) -> impl Iterator<Item = (SlotId, &'a [u8])> + 'a {
        (0..self.slot_count()).filter_map(move |id| self.get_slot(id).map(|d| (id, d)))
    }

    /// Defragment the data region. Live tuples are repacked against the
    /// high end of the page; the slot directory is left in place so slot
    /// ids stay stable.
    pub fn compact(&mut self) {
        let count = self.slot_count();
        let dir_end = PAGE_HEADER_SIZE + count as usize * SLOT_ENTRY_SIZE;
        self.set_free_start(dir_end as u16);

        if count == 0 {
            self.set_free_end(PAGE_SIZE as u16);
            return;
        }

        // Snapshot live tuples (id, bytes).
        let mut live: Vec<(SlotId, Vec<u8>)> = Vec::new();
        for id in 0..count {
            let e = self.read_slot(id);
            if e.is_dead() {
                continue;
            }
            let s = e.offset as usize;
            let l = e.length as usize;
            live.push((id, self.data[s..s + l].to_vec()));
        }

        // Wipe the tuple region (between dir_end and PAGE_SIZE).
        for byte in &mut self.data[dir_end..PAGE_SIZE] {
            *byte = 0;
        }

        // Repack live tuples against the high end of the page.
        let mut cursor = PAGE_SIZE;
        for (id, bytes) in &live {
            let start = cursor - bytes.len();
            self.data[start..cursor].copy_from_slice(bytes);
            let mut e = self.read_slot(*id);
            e.offset = start as u16;
            e.length = bytes.len() as u16;
            self.write_slot(*id, e);
            cursor = start;
        }

        self.set_free_end(cursor as u16);
    }

    /// Whole body slice (everything after the header). Useful for B+Tree
    /// nodes that still serialise themselves into the body region.
    pub fn body(&self) -> &[u8] {
        &self.data[PAGE_HEADER_SIZE..]
    }

    pub fn body_mut(&mut self) -> &mut [u8] {
        &mut self.data[PAGE_HEADER_SIZE..]
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Page")
            .field("id", &self.id)
            .field("type", &self.page_type())
            .field("lsn", &self.lsn())
            .field("slots", &self.slot_count())
            .field("live", &self.live_count())
            .field("free_start", &self.free_start())
            .field("free_end", &self.free_end())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_get_basic() {
        let mut p = Page::new(1);
        let s0 = p.alloc_slot(b"hello").unwrap();
        let s1 = p.alloc_slot(b"world!").unwrap();
        assert_eq!(s0, 0);
        assert_eq!(s1, 1);
        assert_eq!(p.get_slot(s0).unwrap(), b"hello");
        assert_eq!(p.get_slot(s1).unwrap(), b"world!");
        assert_eq!(p.live_count(), 2);
        assert_eq!(p.slot_count(), 2);
    }

    #[test]
    fn delete_marks_tombstone_and_keeps_slot_id_stable() {
        let mut p = Page::new(1);
        let a = p.alloc_slot(b"aaa").unwrap();
        let b = p.alloc_slot(b"bbb").unwrap();
        let c = p.alloc_slot(b"ccc").unwrap();
        p.delete_slot(b).unwrap();
        assert_eq!(p.live_count(), 2);
        assert_eq!(p.slot_count(), 3);
        assert_eq!(p.get_slot(a).unwrap(), b"aaa");
        assert!(p.get_slot(b).is_none());
        assert_eq!(p.get_slot(c).unwrap(), b"ccc");
    }

    #[test]
    fn compact_reclaims_dead_space() {
        let mut p = Page::new(1);
        let big = vec![0xAB; 4000];
        let s0 = p.alloc_slot(&big).unwrap();
        let _ = p.alloc_slot(b"small").unwrap();
        p.delete_slot(s0).unwrap();
        let before = p.free_space();
        p.compact();
        let after = p.free_space();
        assert!(after >= before + 3500, "compact should reclaim big slot");
    }

    #[test]
    fn fills_page_until_oom() {
        let mut p = Page::new(1);
        let chunk = vec![0xCD; 64];
        let mut n = 0;
        while p.alloc_slot(&chunk).is_ok() {
            n += 1;
            if n > 200 {
                break;
            }
        }
        assert!(n > 50, "expected to fit many small tuples, got {}", n);
    }

    #[test]
    fn update_slot_in_place_and_relocate() {
        let mut p = Page::new(1);
        let s = p.alloc_slot(b"original").unwrap();
        p.update_slot(s, b"shorter").unwrap();
        assert_eq!(p.get_slot(s).unwrap(), b"shorter");
        p.update_slot(s, b"a much longer replacement string!").unwrap();
        assert_eq!(
            p.get_slot(s).unwrap(),
            b"a much longer replacement string!"
        );
    }

    #[test]
    fn iter_live_skips_tombstones() {
        let mut p = Page::new(1);
        let _ = p.alloc_slot(b"a").unwrap();
        let b = p.alloc_slot(b"b").unwrap();
        let _ = p.alloc_slot(b"c").unwrap();
        p.delete_slot(b).unwrap();
        let live: Vec<_> = p.iter_live().map(|(_, d)| d.to_vec()).collect();
        assert_eq!(live, vec![b"a".to_vec(), b"c".to_vec()]);
    }
}
