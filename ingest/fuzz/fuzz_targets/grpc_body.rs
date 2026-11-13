//! A gRPC request body through the snappy layer.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| toudai_ingest::fuzzing::grpc_body(data));
