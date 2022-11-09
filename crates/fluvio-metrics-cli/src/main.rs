use std::{
    os::unix::net::UnixStream,
    io::BufReader,
    thread,
    time::{Duration, Instant},
    collections::VecDeque,
};
use fluvio_types::defaults::SPU_MONITORING_UNIX_SOCKET;

fn main() {
    let mut rolling = VecDeque::default();
    loop {
        if let Some(m) = collect() {
            let now = Instant::now();
            rolling.push_back((now, m));
        }

        if rolling.len() >= 2 {
            let (t1, oldest) = rolling.front().unwrap();
            let (t2, newest) = rolling.back().unwrap();
            let delta = *newest - *oldest;
            let delta_t_secs = (*t2 - *t1).as_secs_f64();

            let inbound_bytes_per_sec = (delta.inbound.client.bytes as f64) / delta_t_secs / 1000.;
            let outbound_bytes_per_sec =
                (delta.outbound.client.bytes as f64) / delta_t_secs / 1000.;
            let inbound_records_per_sec = (delta.inbound.client.records as f64) / delta_t_secs;
            let outbound_records_per_sec = (delta.outbound.client.records as f64) / delta_t_secs;

            let total_bytes = newest.inbound.client.bytes as f64 / 1_000_000_000.;
            let total_records = newest.inbound.client.records;

            println!("Inbound: {inbound_bytes_per_sec:8.3} kb/s {inbound_records_per_sec:13.0} r/s    Outbound: {outbound_bytes_per_sec:8.3} kb/s {outbound_records_per_sec:13.0} r/s. total ingres: {total_bytes:6.3} gb. Total records: {total_records:15.0}" );
        }
        if rolling.len() > 30 {
            rolling.pop_front();
        }
        thread::sleep(Duration::from_secs(1));
    }
}

pub fn collect() -> Option<SpuMetrics> {
    let stream = if let Ok(stream) = UnixStream::connect(&SPU_MONITORING_UNIX_SOCKET) {
        stream
    } else {
        println!("unable to connect to fluvio metrics socket");
        return None;
    };
    let reader = BufReader::new(stream);
    let metrics: SpuMetrics = if let Ok(metrics) = serde_json::from_reader(reader) {
        metrics
    } else {
        println!("unable to deserialize fluvio metrics");
        return None;
    };
    Some(metrics)
}

use derive_more::{AddAssign, DivAssign, Sub};
use serde::Deserialize;
#[derive(Clone, Debug, Default, Deserialize, AddAssign, DivAssign, Sub, Copy)]
pub struct SpuMetrics {
    pub inbound: Activity,
    pub outbound: Activity,
    pub smartmodule: SmartModuleChainMetrics,
}

#[derive(Clone, Debug, Default, Deserialize, AddAssign, DivAssign, Sub, Copy)]
pub struct SmartModuleChainMetrics {
    pub bytes_in: u64,
    pub records_out: u64,
    pub invocation_count: u64,
}

#[derive(Clone, Debug, Default, Deserialize, AddAssign, DivAssign, Sub, Copy)]
pub struct Record {
    pub records: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, Default, Deserialize, AddAssign, DivAssign, Sub, Copy)]
pub struct Activity {
    pub connector: Record,
    pub client: Record,
}
