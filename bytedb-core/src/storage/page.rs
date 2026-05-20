use std::fmt;

pub const PAGE_SIZE: usize = 8192;
pub type PageId = u32;

pub const INVALID_PAGE_ID: PageId = 0;

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

pub const PAGE_HEADER_SIZE: usize = 24;

#[derive(Clone)]
pub struct Page {
    pub id: PageId,
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new(id: PageId) -> Self {
        let mut page = Page {
            id,
            data: [0u8; PAGE_SIZE],
        };
        page.set_page_id(id);
        page
    }

    pub fn page_type(&self) -> PageType {
        PageType::from(self.data[4])
    }

    pub fn set_page_type(&mut self, pt: PageType) {
        self.data[4] = pt as u8;
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
        crc32fast::hash(&self.data[PAGE_HEADER_SIZE..])
    }

    pub fn item_count(&self) -> u16 {
        u16::from_le_bytes([self.data[20], self.data[21]])
    }

    pub fn set_item_count(&mut self, count: u16) {
        self.data[20..22].copy_from_slice(&count.to_le_bytes());
    }

    pub fn free_space_offset(&self) -> u16 {
        u16::from_le_bytes([self.data[22], self.data[23]])
    }

    pub fn set_free_space_offset(&mut self, offset: u16) {
        self.data[22..24].copy_from_slice(&offset.to_le_bytes());
    }

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
            .field("item_count", &self.item_count())
            .finish()
    }
}
