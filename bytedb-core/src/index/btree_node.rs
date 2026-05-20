use crate::storage::page::PageId;

pub const BTREE_NODE_HEADER_SIZE: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeType {
    Internal = 0,
    Leaf = 1,
}

#[derive(Debug, Clone)]
pub struct NodeHeader {
    pub node_type: NodeType,
    pub num_keys: u16,
    pub parent: PageId,
    pub right_sibling: PageId,
}

impl NodeHeader {
    pub fn serialize(&self, buf: &mut [u8]) {
        buf[0] = self.node_type as u8;
        buf[1..3].copy_from_slice(&self.num_keys.to_le_bytes());
        buf[3..7].copy_from_slice(&self.parent.to_le_bytes());
        buf[7..11].copy_from_slice(&self.right_sibling.to_le_bytes());
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        NodeHeader {
            node_type: if buf[0] == 0 { NodeType::Internal } else { NodeType::Leaf },
            num_keys: u16::from_le_bytes([buf[1], buf[2]]),
            parent: u32::from_le_bytes([buf[3], buf[4], buf[5], buf[6]]),
            right_sibling: u32::from_le_bytes([buf[7], buf[8], buf[9], buf[10]]),
        }
    }
}
