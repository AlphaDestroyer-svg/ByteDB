use std::io::{Read, Write};
use crate::error::{CoreError, Result};
use super::format::FullSnapshot;

pub fn serialize_snapshot<W: Write>(snapshot: &FullSnapshot, writer: &mut W) -> Result<()> {
    serde_json::to_writer_pretty(writer, snapshot)
        .map_err(|e| CoreError::Internal(e.to_string()))
}

pub fn deserialize_snapshot<R: Read>(reader: &mut R) -> Result<FullSnapshot> {
    serde_json::from_reader(reader)
        .map_err(|e| CoreError::Internal(e.to_string()))
}
