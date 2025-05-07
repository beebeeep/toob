pub mod node;
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/toob.protocol.rs"));
}
