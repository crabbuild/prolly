use js_sys::{Object, Reflect, Uint8Array};
use wasm_bindgen::prelude::*;

pub(crate) fn set_bytes(object: &Object, field: &str, value: &[u8]) -> Result<(), JsValue> {
    Reflect::set(
        object,
        &JsValue::from_str(field),
        &Uint8Array::from(value).into(),
    )?;
    Ok(())
}

pub(crate) fn set_optional_bytes(
    object: &Object,
    field: &str,
    value: Option<&[u8]>,
) -> Result<(), JsValue> {
    match value {
        Some(value) => set_bytes(object, field, value),
        None => {
            Reflect::set(object, &JsValue::from_str(field), &JsValue::UNDEFINED)?;
            Ok(())
        }
    }
}
