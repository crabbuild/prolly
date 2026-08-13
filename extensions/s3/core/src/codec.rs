use std::{cmp::Ordering, fmt};

use minicbor::{data::Type, decode::Decoder, encode::Encoder};
use serde::{
    de::{
        self, value::Error as ValueError, DeserializeOwned, DeserializeSeed, Deserializer as _,
        EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor,
    },
    ser::{
        self, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
    Serialize,
};
use sha2::{Digest, Sha256};

use crate::{Error, ErrorCode, Result};

const MAX_CBOR_DEPTH: usize = 128;

/// Encode the frozen Prolly S3 wire profile with Minicbor.
///
/// Struct fields and unit/struct enum variants use their stable numeric Serde
/// indices, matching the former `serde_cbor` packed representation. Maps are
/// buffered and sorted using deterministic CBOR key ordering. Persisted
/// protocol types intentionally contain no floating-point values, tags, or
/// negative integers.
pub fn encode_canonical<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    value
        .serialize(WireSerializer)
        .map_err(|error| Error::serialization(format!("canonical encode failed: {error}")))
}

/// Decode and reject alternate encodings of the same value.
pub fn decode_canonical<T>(bytes: &[u8]) -> Result<T>
where
    T: DeserializeOwned + Serialize,
{
    let wire = decode_wire_value(bytes).map_err(|error| {
        Error::new(
            ErrorCode::CorruptCommit,
            format!("canonical decode failed: {error}"),
        )
    })?;
    let value = T::deserialize(wire).map_err(|error| {
        Error::new(
            ErrorCode::CorruptCommit,
            format!("canonical decode failed: {error}"),
        )
    })?;
    let encoded = encode_canonical(&value)?;
    if encoded != bytes {
        return Err(Error::new(
            ErrorCode::CorruptCommit,
            "noncanonical CBOR encoding",
        ));
    }
    Ok(value)
}

#[derive(Debug)]
struct WireError(String);

impl fmt::Display for WireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for WireError {}

impl ser::Error for WireError {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self(message.to_string())
    }
}

type WireResult<T> = std::result::Result<T, WireError>;

#[derive(Clone, Copy)]
struct WireSerializer;

impl WireSerializer {
    fn scalar(
        encode: impl FnOnce(
            &mut Encoder<&mut Vec<u8>>,
        ) -> std::result::Result<
            (),
            minicbor::encode::Error<std::convert::Infallible>,
        >,
    ) -> WireResult<Vec<u8>> {
        let mut output = Vec::new();
        encode(&mut Encoder::new(&mut output)).map_err(|error| WireError(error.to_string()))?;
        Ok(output)
    }

    fn unsigned(value: u64) -> WireResult<Vec<u8>> {
        Self::scalar(|encoder| encoder.u64(value).map(|_| ()))
    }

    fn text(value: &str) -> WireResult<Vec<u8>> {
        Self::scalar(|encoder| encoder.str(value).map(|_| ()))
    }
}

impl ser::Serializer for WireSerializer {
    type Ok = Vec<u8>;
    type Error = WireError;
    type SerializeSeq = SequenceSerializer;
    type SerializeTuple = SequenceSerializer;
    type SerializeTupleStruct = SequenceSerializer;
    type SerializeTupleVariant = SequenceSerializer;
    type SerializeMap = MapSerializer;
    type SerializeStruct = StructSerializer;
    type SerializeStructVariant = StructSerializer;

    fn serialize_bool(self, value: bool) -> WireResult<Self::Ok> {
        Self::scalar(|encoder| encoder.bool(value).map(|_| ()))
    }

    fn serialize_i8(self, value: i8) -> WireResult<Self::Ok> {
        self.serialize_i128(i128::from(value))
    }

    fn serialize_i16(self, value: i16) -> WireResult<Self::Ok> {
        self.serialize_i128(i128::from(value))
    }

    fn serialize_i32(self, value: i32) -> WireResult<Self::Ok> {
        self.serialize_i128(i128::from(value))
    }

    fn serialize_i64(self, value: i64) -> WireResult<Self::Ok> {
        self.serialize_i128(i128::from(value))
    }

    fn serialize_i128(self, value: i128) -> WireResult<Self::Ok> {
        if value < 0 {
            return Err(WireError("Prolly S3 forbids negative CBOR integers".into()));
        }
        let value = u64::try_from(value)
            .map_err(|_| WireError("CBOR integer exceeds the Prolly S3 wire profile".into()))?;
        Self::unsigned(value)
    }

    fn serialize_u8(self, value: u8) -> WireResult<Self::Ok> {
        Self::unsigned(u64::from(value))
    }

    fn serialize_u16(self, value: u16) -> WireResult<Self::Ok> {
        Self::unsigned(u64::from(value))
    }

    fn serialize_u32(self, value: u32) -> WireResult<Self::Ok> {
        Self::unsigned(u64::from(value))
    }

    fn serialize_u64(self, value: u64) -> WireResult<Self::Ok> {
        Self::unsigned(value)
    }

    fn serialize_u128(self, value: u128) -> WireResult<Self::Ok> {
        let value = u64::try_from(value)
            .map_err(|_| WireError("CBOR integer exceeds the Prolly S3 wire profile".into()))?;
        Self::unsigned(value)
    }

    fn serialize_f32(self, _value: f32) -> WireResult<Self::Ok> {
        Err(WireError(
            "Prolly S3 forbids CBOR floating-point values".into(),
        ))
    }

    fn serialize_f64(self, _value: f64) -> WireResult<Self::Ok> {
        Err(WireError(
            "Prolly S3 forbids CBOR floating-point values".into(),
        ))
    }

    fn serialize_char(self, value: char) -> WireResult<Self::Ok> {
        Self::text(value.encode_utf8(&mut [0; 4]))
    }

    fn serialize_str(self, value: &str) -> WireResult<Self::Ok> {
        Self::text(value)
    }

    fn serialize_bytes(self, value: &[u8]) -> WireResult<Self::Ok> {
        Self::scalar(|encoder| encoder.bytes(value).map(|_| ()))
    }

    fn serialize_none(self) -> WireResult<Self::Ok> {
        Self::scalar(|encoder| encoder.null().map(|_| ()))
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> WireResult<Self::Ok> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> WireResult<Self::Ok> {
        self.serialize_none()
    }

    fn serialize_unit_struct(self, _name: &'static str) -> WireResult<Self::Ok> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
    ) -> WireResult<Self::Ok> {
        Self::unsigned(u64::from(variant_index))
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> WireResult<Self::Ok> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> WireResult<Self::Ok> {
        encode_map(vec![(Self::text(variant)?, value.serialize(self)?)])
    }

    fn serialize_seq(self, len: Option<usize>) -> WireResult<Self::SerializeSeq> {
        Ok(SequenceSerializer::new(len, None))
    }

    fn serialize_tuple(self, len: usize) -> WireResult<Self::SerializeTuple> {
        Ok(SequenceSerializer::new(Some(len), None))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> WireResult<Self::SerializeTupleStruct> {
        self.serialize_tuple(len)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> WireResult<Self::SerializeTupleVariant> {
        Ok(SequenceSerializer::new(
            Some(len),
            Some(Self::text(variant)?),
        ))
    }

    fn serialize_map(self, len: Option<usize>) -> WireResult<Self::SerializeMap> {
        Ok(MapSerializer::new(len))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> WireResult<Self::SerializeStruct> {
        Ok(StructSerializer::new(len, None))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        _variant: &'static str,
        len: usize,
    ) -> WireResult<Self::SerializeStructVariant> {
        Ok(StructSerializer::new(
            len,
            Some(Self::unsigned(u64::from(variant_index))?),
        ))
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

struct SequenceSerializer {
    expected: Option<usize>,
    elements: Vec<Vec<u8>>,
    variant: Option<Vec<u8>>,
}

impl SequenceSerializer {
    fn new(expected: Option<usize>, variant: Option<Vec<u8>>) -> Self {
        Self {
            expected,
            elements: Vec::with_capacity(expected.unwrap_or_default()),
            variant,
        }
    }

    fn push<T: ?Sized + Serialize>(&mut self, value: &T) -> WireResult<()> {
        self.elements.push(value.serialize(WireSerializer)?);
        Ok(())
    }

    fn finish(self) -> WireResult<Vec<u8>> {
        if self
            .expected
            .is_some_and(|expected| expected != self.elements.len())
        {
            return Err(WireError("serialized sequence length changed".into()));
        }
        let array = encode_array(self.elements)?;
        match self.variant {
            Some(variant) => encode_map(vec![(variant, array)]),
            None => Ok(array),
        }
    }
}

impl SerializeSeq for SequenceSerializer {
    type Ok = Vec<u8>;
    type Error = WireError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> WireResult<()> {
        self.push(value)
    }

    fn end(self) -> WireResult<Self::Ok> {
        self.finish()
    }
}

impl SerializeTuple for SequenceSerializer {
    type Ok = Vec<u8>;
    type Error = WireError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> WireResult<()> {
        self.push(value)
    }

    fn end(self) -> WireResult<Self::Ok> {
        self.finish()
    }
}

impl SerializeTupleStruct for SequenceSerializer {
    type Ok = Vec<u8>;
    type Error = WireError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> WireResult<()> {
        self.push(value)
    }

    fn end(self) -> WireResult<Self::Ok> {
        self.finish()
    }
}

impl SerializeTupleVariant for SequenceSerializer {
    type Ok = Vec<u8>;
    type Error = WireError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> WireResult<()> {
        self.push(value)
    }

    fn end(self) -> WireResult<Self::Ok> {
        self.finish()
    }
}

struct MapSerializer {
    expected: Option<usize>,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    pending_key: Option<Vec<u8>>,
}

impl MapSerializer {
    fn new(expected: Option<usize>) -> Self {
        Self {
            expected,
            entries: Vec::with_capacity(expected.unwrap_or_default()),
            pending_key: None,
        }
    }
}

impl SerializeMap for MapSerializer {
    type Ok = Vec<u8>;
    type Error = WireError;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> WireResult<()> {
        if self.pending_key.is_some() {
            return Err(WireError("serialized map omitted a value".into()));
        }
        self.pending_key = Some(key.serialize(WireSerializer)?);
        Ok(())
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> WireResult<()> {
        let key = self
            .pending_key
            .take()
            .ok_or_else(|| WireError("serialized map value has no key".into()))?;
        self.entries.push((key, value.serialize(WireSerializer)?));
        Ok(())
    }

    fn end(self) -> WireResult<Self::Ok> {
        if self.pending_key.is_some() {
            return Err(WireError("serialized map omitted a value".into()));
        }
        if self
            .expected
            .is_some_and(|expected| expected != self.entries.len())
        {
            return Err(WireError("serialized map length changed".into()));
        }
        encode_map(self.entries)
    }
}

struct StructSerializer {
    expected: usize,
    index: u32,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    variant: Option<Vec<u8>>,
}

impl StructSerializer {
    fn new(expected: usize, variant: Option<Vec<u8>>) -> Self {
        Self {
            expected,
            index: 0,
            entries: Vec::with_capacity(expected),
            variant,
        }
    }

    fn field<T: ?Sized + Serialize>(&mut self, value: &T) -> WireResult<()> {
        let key = WireSerializer::unsigned(u64::from(self.index))?;
        self.entries.push((key, value.serialize(WireSerializer)?));
        self.index = self
            .index
            .checked_add(1)
            .ok_or_else(|| WireError("struct field index overflow".into()))?;
        Ok(())
    }

    fn skip(&mut self) -> WireResult<()> {
        self.index = self
            .index
            .checked_add(1)
            .ok_or_else(|| WireError("struct field index overflow".into()))?;
        Ok(())
    }

    fn finish(self) -> WireResult<Vec<u8>> {
        if self.entries.len() != self.expected {
            return Err(WireError("serialized struct length changed".into()));
        }
        let map = encode_map(self.entries)?;
        match self.variant {
            Some(variant) => encode_map(vec![(variant, map)]),
            None => Ok(map),
        }
    }
}

impl SerializeStruct for StructSerializer {
    type Ok = Vec<u8>;
    type Error = WireError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> WireResult<()> {
        self.field(value)
    }

    fn skip_field(&mut self, _key: &'static str) -> WireResult<()> {
        self.skip()
    }

    fn end(self) -> WireResult<Self::Ok> {
        self.finish()
    }
}

impl SerializeStructVariant for StructSerializer {
    type Ok = Vec<u8>;
    type Error = WireError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> WireResult<()> {
        self.field(value)
    }

    fn skip_field(&mut self, _key: &'static str) -> WireResult<()> {
        self.skip()
    }

    fn end(self) -> WireResult<Self::Ok> {
        self.finish()
    }
}

fn encode_array(elements: Vec<Vec<u8>>) -> WireResult<Vec<u8>> {
    let mut output = Vec::new();
    Encoder::new(&mut output)
        .array(elements.len() as u64)
        .map_err(|error| WireError(error.to_string()))?;
    for element in elements {
        output.extend_from_slice(&element);
    }
    Ok(output)
}

fn encode_map(mut entries: Vec<(Vec<u8>, Vec<u8>)>) -> WireResult<Vec<u8>> {
    entries.sort_by(|left, right| canonical_key_cmp(&left.0, &right.0));
    if entries.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(WireError("duplicate canonical CBOR map key".into()));
    }
    let mut output = Vec::new();
    Encoder::new(&mut output)
        .map(entries.len() as u64)
        .map_err(|error| WireError(error.to_string()))?;
    for (key, value) in entries {
        output.extend_from_slice(&key);
        output.extend_from_slice(&value);
    }
    Ok(output)
}

fn canonical_key_cmp(left: &[u8], right: &[u8]) -> Ordering {
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

#[derive(Debug)]
enum WireValue {
    Null,
    Bool(bool),
    Integer(i128),
    Bytes(Vec<u8>),
    Text(String),
    Array(Vec<WireValue>),
    Map(Vec<(WireValue, WireValue)>),
}

fn decode_wire_value(bytes: &[u8]) -> std::result::Result<WireValue, minicbor::decode::Error> {
    let mut decoder = Decoder::new(bytes);
    let value = decode_wire_item(&mut decoder, 0)?;
    if decoder.position() != bytes.len() {
        return Err(minicbor::decode::Error::message("trailing CBOR data"));
    }
    Ok(value)
}

fn decode_wire_item(
    decoder: &mut Decoder<'_>,
    depth: usize,
) -> std::result::Result<WireValue, minicbor::decode::Error> {
    if depth >= MAX_CBOR_DEPTH {
        return Err(minicbor::decode::Error::message(
            "CBOR nesting exceeds the Prolly S3 limit",
        ));
    }
    match decoder.datatype()? {
        Type::Null => {
            decoder.null()?;
            Ok(WireValue::Null)
        }
        Type::Bool => Ok(WireValue::Bool(decoder.bool()?)),
        Type::U8 | Type::U16 | Type::U32 | Type::U64 => {
            Ok(WireValue::Integer(i128::from(decoder.u64()?)))
        }
        Type::I8 | Type::I16 | Type::I32 | Type::I64 | Type::Int => {
            Ok(WireValue::Integer(decoder.i128()?))
        }
        Type::Bytes => Ok(WireValue::Bytes(decoder.bytes()?.to_vec())),
        Type::String => Ok(WireValue::Text(decoder.str()?.to_owned())),
        Type::Array => {
            let len = decoder.array()?.expect("definite array has a length");
            let len = usize::try_from(len)
                .map_err(|_| minicbor::decode::Error::message("CBOR array is too large"))?;
            let remaining = decoder.input().len().saturating_sub(decoder.position());
            if len > remaining {
                return Err(minicbor::decode::Error::message(
                    "CBOR array length exceeds the input",
                ));
            }
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                values.push(decode_wire_item(decoder, depth + 1)?);
            }
            Ok(WireValue::Array(values))
        }
        Type::Map => {
            let len = decoder.map()?.expect("definite map has a length");
            let len = usize::try_from(len)
                .map_err(|_| minicbor::decode::Error::message("CBOR map is too large"))?;
            let remaining = decoder.input().len().saturating_sub(decoder.position());
            if len > remaining / 2 {
                return Err(minicbor::decode::Error::message(
                    "CBOR map length exceeds the input",
                ));
            }
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                let key = decode_wire_item(decoder, depth + 1)?;
                let value = decode_wire_item(decoder, depth + 1)?;
                values.push((key, value));
            }
            Ok(WireValue::Map(values))
        }
        Type::F16 | Type::F32 | Type::F64 => Err(minicbor::decode::Error::message(
            "Prolly S3 forbids CBOR floating-point values",
        )),
        Type::Tag => Err(minicbor::decode::Error::message(
            "Prolly S3 forbids CBOR semantic tags",
        )),
        Type::ArrayIndef | Type::MapIndef | Type::BytesIndef | Type::StringIndef => Err(
            minicbor::decode::Error::message("Prolly S3 requires definite-length CBOR values"),
        ),
        other => Err(minicbor::decode::Error::message(format!(
            "unsupported Prolly S3 CBOR value {other}"
        ))),
    }
}

impl<'de> de::Deserializer<'de> for WireValue {
    type Error = ValueError;

    fn deserialize_any<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        match self {
            Self::Null => visitor.visit_none(),
            Self::Bool(value) => visitor.visit_bool(value),
            Self::Integer(value) if value >= 0 => {
                visitor.visit_u64(u64::try_from(value).map_err(de::Error::custom)?)
            }
            Self::Integer(value) => visitor.visit_i128(value),
            Self::Bytes(value) => visitor.visit_byte_buf(value),
            Self::Text(value) => visitor.visit_string(value),
            Self::Array(values) => visitor.visit_seq(WireSeqAccess {
                values: values.into_iter(),
            }),
            Self::Map(values) => visitor.visit_map(WireMapAccess {
                values: values.into_iter(),
                pending: None,
            }),
        }
    }

    fn deserialize_option<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        match self {
            Self::Null => visitor.visit_none(),
            value => visitor.visit_some(value),
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        match self {
            Self::Null => visitor.visit_unit(),
            value => Err(de::Error::invalid_type(value.unexpected(), &"null")),
        }
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        match self {
            Self::Array(values) => visitor.visit_seq(WireSeqAccess {
                values: values.into_iter(),
            }),
            value => Err(de::Error::invalid_type(value.unexpected(), &"a CBOR array")),
        }
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        match self {
            Self::Array(values) if values.len() == len => visitor.visit_seq(WireSeqAccess {
                values: values.into_iter(),
            }),
            Self::Array(values) => Err(de::Error::invalid_length(
                values.len(),
                &"the expected tuple length",
            )),
            value => Err(de::Error::invalid_type(value.unexpected(), &"a CBOR array")),
        }
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        match self {
            Self::Map(values) => visitor.visit_map(WireMapAccess {
                values: values.into_iter(),
                pending: None,
            }),
            value => Err(de::Error::invalid_type(value.unexpected(), &"a CBOR map")),
        }
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        match self {
            Self::Map(mut values) if values.len() == 1 => {
                let (variant, value) = values.pop().expect("one enum map entry");
                visitor.visit_enum(WireEnumAccess {
                    variant,
                    value: Some(value),
                })
            }
            Self::Map(values) => Err(de::Error::invalid_length(
                values.len(),
                &"a one-entry enum map",
            )),
            variant @ (Self::Integer(_) | Self::Text(_)) => visitor.visit_enum(WireEnumAccess {
                variant,
                value: None,
            }),
            value => Err(de::Error::invalid_type(value.unexpected(), &"a CBOR enum")),
        }
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        visitor.visit_unit()
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf identifier
    }
}

impl WireValue {
    fn unexpected(&self) -> de::Unexpected<'_> {
        match self {
            Self::Null => de::Unexpected::Option,
            Self::Bool(value) => de::Unexpected::Bool(*value),
            Self::Integer(value) if *value >= 0 => u64::try_from(*value).map_or(
                de::Unexpected::Other("large integer"),
                de::Unexpected::Unsigned,
            ),
            Self::Integer(value) => i64::try_from(*value).map_or(
                de::Unexpected::Other("large negative integer"),
                de::Unexpected::Signed,
            ),
            Self::Bytes(value) => de::Unexpected::Bytes(value),
            Self::Text(value) => de::Unexpected::Str(value),
            Self::Array(_) => de::Unexpected::Seq,
            Self::Map(_) => de::Unexpected::Map,
        }
    }
}

struct WireSeqAccess {
    values: std::vec::IntoIter<WireValue>,
}

impl<'de> SeqAccess<'de> for WireSeqAccess {
    type Error = ValueError;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> std::result::Result<Option<T::Value>, Self::Error> {
        self.values
            .next()
            .map(|value| seed.deserialize(value))
            .transpose()
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.values.len())
    }
}

struct WireMapAccess {
    values: std::vec::IntoIter<(WireValue, WireValue)>,
    pending: Option<WireValue>,
}

impl<'de> MapAccess<'de> for WireMapAccess {
    type Error = ValueError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> std::result::Result<Option<K::Value>, Self::Error> {
        match self.values.next() {
            Some((key, value)) => {
                self.pending = Some(value);
                seed.deserialize(key).map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        seed.deserialize(
            self.pending
                .take()
                .ok_or_else(|| de::Error::custom("CBOR map value has no key"))?,
        )
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.values.len())
    }
}

struct WireEnumAccess {
    variant: WireValue,
    value: Option<WireValue>,
}

impl<'de> EnumAccess<'de> for WireEnumAccess {
    type Error = ValueError;
    type Variant = WireVariantAccess;

    fn variant_seed<V: DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> std::result::Result<(V::Value, Self::Variant), Self::Error> {
        let variant = seed.deserialize(self.variant)?;
        Ok((variant, WireVariantAccess { value: self.value }))
    }
}

struct WireVariantAccess {
    value: Option<WireValue>,
}

impl<'de> VariantAccess<'de> for WireVariantAccess {
    type Error = ValueError;

    fn unit_variant(self) -> std::result::Result<(), Self::Error> {
        match self.value {
            None | Some(WireValue::Null) => Ok(()),
            Some(value) => Err(de::Error::invalid_type(
                value.unexpected(),
                &"a unit variant",
            )),
        }
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> std::result::Result<T::Value, Self::Error> {
        seed.deserialize(self.value.ok_or_else(|| {
            de::Error::invalid_type(de::Unexpected::UnitVariant, &"a newtype variant")
        })?)
    }

    fn tuple_variant<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        self.value
            .ok_or_else(|| {
                de::Error::invalid_type(de::Unexpected::UnitVariant, &"a tuple variant")
            })?
            .deserialize_tuple(len, visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error> {
        self.value
            .ok_or_else(|| {
                de::Error::invalid_type(de::Unexpected::UnitVariant, &"a struct variant")
            })?
            .deserialize_struct("variant", fields, visitor)
    }
}

pub(crate) fn domain_hash(domain: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update((domain.len() as u32).to_be_bytes());
    hasher.update(domain);
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    hasher.finalize().into()
}

pub(crate) fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Fixture {
        version: u16,
        metadata: BTreeMap<String, Vec<u8>>,
    }

    #[test]
    fn canonical_maps_use_cbor_key_order_not_rust_string_order() {
        let fixture = Fixture {
            version: 1,
            metadata: BTreeMap::from([("aa".to_string(), vec![2]), ("z".to_string(), vec![1])]),
        };
        let encoded = encode_canonical(&fixture).unwrap();
        assert_eq!(hex::encode(&encoded), "a2000101a2617a81016261618102");
        assert_eq!(decode_canonical::<Fixture>(&encoded).unwrap(), fixture);
    }

    #[test]
    fn decoder_rejects_a_different_map_order() {
        // {0: 1, 1: {"aa": [2], "z": [1]}} with the inner text keys in
        // noncanonical order ("z" has the shorter encoded key).
        let noncanonical = hex::decode("a2000101a26261618102617a8101").unwrap();
        assert_eq!(
            decode_canonical::<Fixture>(&noncanonical).unwrap_err().code,
            ErrorCode::CorruptCommit
        );
    }

    #[test]
    fn decoder_rejects_trailing_and_deeply_nested_data() {
        assert_eq!(
            decode_canonical::<u8>(&[0x01, 0x02]).unwrap_err().code,
            ErrorCode::CorruptCommit
        );

        let mut nested = vec![0x81; MAX_CBOR_DEPTH + 1];
        nested.push(0x00);
        assert!(decode_wire_value(&nested).is_err());

        // A forged collection length must fail before allocating from it.
        assert!(decode_wire_value(&hex::decode("9bffffffffffffffff").unwrap()).is_err());
    }

    #[test]
    fn wire_profile_rejects_forbidden_scalar_types() {
        assert_eq!(
            encode_canonical(&-1_i8).unwrap_err().code,
            ErrorCode::InternalInvariant
        );
        assert_eq!(
            encode_canonical(&1.0_f64).unwrap_err().code,
            ErrorCode::InternalInvariant
        );
        assert_eq!(
            decode_canonical::<u8>(&[0xc0, 0x01]).unwrap_err().code,
            ErrorCode::CorruptCommit
        );
    }
}
