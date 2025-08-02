use std::io::{Read, Seek};

use crate::err::Result;
use crate::utils;
use serde::ser;

/// $Data Attribute
#[derive(Clone, Debug)]
pub struct DataAttr(Vec<u8>);

impl DataAttr {
    pub fn from_stream<S: Read + Seek>(stream: &mut S, data_size: usize) -> Result<DataAttr> {
        let mut data = vec![0_u8; data_size];

        stream.read_exact(&mut data)?;

        Ok(DataAttr(data))
    }

    pub fn data(&self) -> &[u8] {
        &self.0
    }
}

impl ser::Serialize for DataAttr {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&utils::to_hex_string(&self.0))
    }
}
