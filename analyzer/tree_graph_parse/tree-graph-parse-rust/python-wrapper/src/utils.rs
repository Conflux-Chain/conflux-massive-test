use ethereum_types::H256;
use pyo3::{
    prelude::*,
    types::{PyBytes, PyString},
};

#[allow(dead_code)]
pub fn parse_h256(input: &PyAny) -> PyResult<H256> {
    // Try to extract as bytes first
    if let Ok(bytes) = input.extract::<&PyBytes>() {
        let bytes_slice = bytes.as_bytes();
        if bytes_slice.len() == 32 {
            let mut array = [0u8; 32];
            array.copy_from_slice(bytes_slice);
            return Ok(H256(array));
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Bytes input must be exactly 32 bytes long",
            ));
        }
    }

    // Try to extract as string
    if let Ok(string) = input.extract::<&PyString>() {
        let s = string.to_string();
        let hex_str = s.trim_start_matches("0x");

        match hex::decode(hex_str) {
            Ok(bytes) => {
                if bytes.len() == 32 {
                    let mut array = [0u8; 32];
                    array.copy_from_slice(&bytes);
                    Ok(H256(array))
                } else {
                    Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Hex string must represent exactly 32 bytes (64 hex characters)",
                    ))
                }
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Invalid hex string: {}",
                e
            ))),
        }
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Input must be either a string or bytes object",
        ))
    }
}
