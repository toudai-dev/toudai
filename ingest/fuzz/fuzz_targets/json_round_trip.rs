//! A protobuf request, written as OTLP/JSON with NaN and the infinities
//! spelled as pdata spells them, decodes unchanged.
//! The first byte picks the signal and how doubles are written; see
//! `toudai_ingest::fuzzing::json_round_trip`.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Some((&selector, protobuf)) = data.split_first() {
        toudai_ingest::fuzzing::json_round_trip(selector, protobuf);
    }
});
