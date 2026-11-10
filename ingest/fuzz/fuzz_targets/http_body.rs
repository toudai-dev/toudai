//! An OTLP/HTTP body, decompressed, decoded, admitted, and mapped. The first
//! byte picks the encoding, format, signal, and key; see
//! `toudai_ingest::fuzzing::http_body`.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Some((&selector, body)) = data.split_first() {
        toudai_ingest::fuzzing::http_body(selector, body);
    }
});
