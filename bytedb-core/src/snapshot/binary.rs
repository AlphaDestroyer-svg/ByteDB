use std::io::{Read, Write};
use crate::error::{CoreError, Result};
use super::format::*;

pub fn serialize_snapshot<W: Write>(snapshot: &FullSnapshot, writer: &mut W) -> Result<()> {
    writer.write_all(&SNAPSHOT_MAGIC)
        .map_err(|e| CoreError::Internal(e.to_string()))?;
    writer.write_all(&SNAPSHOT_VERSION.to_le_bytes())
        .map_err(|e| CoreError::Internal(e.to_string()))?;
    writer.write_all(&snapshot.header.lsn.to_le_bytes())
        .map_err(|e| CoreError::Internal(e.to_string()))?;
    writer.write_all(&snapshot.header.timestamp.to_le_bytes())
        .map_err(|e| CoreError::Internal(e.to_string()))?;
    writer.write_all(&snapshot.header.table_count.to_le_bytes())
        .map_err(|e| CoreError::Internal(e.to_string()))?;

    for table in &snapshot.tables {
        let schema_bytes = serde_json::to_vec(&table.schema)
            .map_err(|e| CoreError::Internal(e.to_string()))?;

        let name_bytes = table.name.as_bytes();
        writer.write_all(&(name_bytes.len() as u32).to_le_bytes())
            .map_err(|e| CoreError::Internal(e.to_string()))?;
        writer.write_all(name_bytes)
            .map_err(|e| CoreError::Internal(e.to_string()))?;

        writer.write_all(&table.table_id.to_le_bytes())
            .map_err(|e| CoreError::Internal(e.to_string()))?;

        writer.write_all(&(schema_bytes.len() as u32).to_le_bytes())
            .map_err(|e| CoreError::Internal(e.to_string()))?;
        writer.write_all(&schema_bytes)
            .map_err(|e| CoreError::Internal(e.to_string()))?;

        writer.write_all(&(table.entries.len() as u64).to_le_bytes())
            .map_err(|e| CoreError::Internal(e.to_string()))?;

        for (key, value) in &table.entries {
            writer.write_all(&(key.len() as u32).to_le_bytes())
                .map_err(|e| CoreError::Internal(e.to_string()))?;
            writer.write_all(key)
                .map_err(|e| CoreError::Internal(e.to_string()))?;
            writer.write_all(&(value.len() as u32).to_le_bytes())
                .map_err(|e| CoreError::Internal(e.to_string()))?;
            writer.write_all(value)
                .map_err(|e| CoreError::Internal(e.to_string()))?;
        }
    }

    let checksum = crc32fast::hash(&[]);
    writer.write_all(&checksum.to_le_bytes())
        .map_err(|e| CoreError::Internal(e.to_string()))?;

    Ok(())
}

pub fn deserialize_snapshot<R: Read>(reader: &mut R) -> Result<FullSnapshot> {
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic)
        .map_err(|e| CoreError::Internal(e.to_string()))?;
    if magic != SNAPSHOT_MAGIC {
        return Err(CoreError::Internal("Invalid snapshot magic".into()));
    }

    let mut buf4 = [0u8; 4];
    let mut buf8 = [0u8; 8];

    reader.read_exact(&mut buf4).map_err(|e| CoreError::Internal(e.to_string()))?;
    let version = u32::from_le_bytes(buf4);
    if version != SNAPSHOT_VERSION {
        return Err(CoreError::Internal(format!("Unsupported snapshot version: {}", version)));
    }

    reader.read_exact(&mut buf8).map_err(|e| CoreError::Internal(e.to_string()))?;
    let lsn = u64::from_le_bytes(buf8);

    reader.read_exact(&mut buf8).map_err(|e| CoreError::Internal(e.to_string()))?;
    let timestamp = i64::from_le_bytes(buf8);

    reader.read_exact(&mut buf4).map_err(|e| CoreError::Internal(e.to_string()))?;
    let table_count = u32::from_le_bytes(buf4);

    let mut tables = Vec::with_capacity(table_count as usize);
    for _ in 0..table_count {
        reader.read_exact(&mut buf4).map_err(|e| CoreError::Internal(e.to_string()))?;
        let name_len = u32::from_le_bytes(buf4) as usize;
        let mut name_bytes = vec![0u8; name_len];
        reader.read_exact(&mut name_bytes).map_err(|e| CoreError::Internal(e.to_string()))?;
        let name = String::from_utf8(name_bytes)
            .map_err(|e| CoreError::Internal(e.to_string()))?;

        reader.read_exact(&mut buf4).map_err(|e| CoreError::Internal(e.to_string()))?;
        let table_id = u32::from_le_bytes(buf4);

        reader.read_exact(&mut buf4).map_err(|e| CoreError::Internal(e.to_string()))?;
        let schema_len = u32::from_le_bytes(buf4) as usize;
        let mut schema_bytes = vec![0u8; schema_len];
        reader.read_exact(&mut schema_bytes).map_err(|e| CoreError::Internal(e.to_string()))?;
        let schema = serde_json::from_slice(&schema_bytes)
            .map_err(|e| CoreError::Internal(e.to_string()))?;

        reader.read_exact(&mut buf8).map_err(|e| CoreError::Internal(e.to_string()))?;
        let entry_count = u64::from_le_bytes(buf8) as usize;

        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            reader.read_exact(&mut buf4).map_err(|e| CoreError::Internal(e.to_string()))?;
            let key_len = u32::from_le_bytes(buf4) as usize;
            let mut key = vec![0u8; key_len];
            reader.read_exact(&mut key).map_err(|e| CoreError::Internal(e.to_string()))?;

            reader.read_exact(&mut buf4).map_err(|e| CoreError::Internal(e.to_string()))?;
            let val_len = u32::from_le_bytes(buf4) as usize;
            let mut value = vec![0u8; val_len];
            reader.read_exact(&mut value).map_err(|e| CoreError::Internal(e.to_string()))?;

            entries.push((key, value));
        }

        tables.push(TableSnapshot { name, table_id, schema, entries });
    }

    Ok(FullSnapshot {
        header: SnapshotHeader { version, lsn, timestamp, table_count },
        tables,
    })
}
