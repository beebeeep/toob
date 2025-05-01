use anyhow::Result;
use bytes::Buf;
use bytes_varint::VarIntSupport;

pub(crate) fn read_nullable_string(mut buf: &[u8]) -> Result<Option<String>> {
    let len = buf.try_get_i16()?;
    if len == -1 {
        return Ok(None);
    }
    Ok(Some(String::from_utf8(
        buf.copy_to_bytes(len as usize).to_vec(),
    )?))
}

pub(crate) fn read_compact_string(mut buf: &[u8]) -> Result<String> {
    let len = buf.try_get_i32_varint()?;
    Ok(String::from_utf8(buf.copy_to_bytes(len as usize).to_vec())?)
}
