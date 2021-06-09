pub mod geoip;

use std::error::Error;

use influx_db_client::{Client as InfluxDbClient, Point};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt as _, BufReader},
    net::TcpStream,
};

use geoip::GeoIPResolver;

#[derive(Clone, Serialize, Deserialize)]
struct AuthAttempt {
    username: String,
    ip: String,
    port: String,
}

pub struct AuthAttemptRecord {
    s2_cell: String,
    username: String,
    ip: String,
}

pub struct Pipeline {
    pub influxdb_client: InfluxDbClient,
    pub geoip_resolver: Box<dyn GeoIPResolver + Sync + Send + 'static>,
    pub s2_precision: usize,
    pub retention_policy: Option<String>,
}

impl Pipeline {
    pub async fn handle_client(&self, client: TcpStream) -> Result<(), Box<dyn Error>> {
        let mut reader = BufReader::new(client);

        let mut line = String::new();
        loop {
            reader.read_line(&mut line).await?;
            let attempt: AuthAttempt = serde_json::from_str(line.trim())?;
            line.clear();

            if attempt.ip == "**NO MATCH**" {
                continue;
            }

            let s2_cell = match self.get_s2_cell(&attempt.ip).await {
                Ok(hash) => hash,
                Err(e) => {
                    eprintln!("Could not get geohash for {}: {}", attempt.ip, e);
                    continue;
                }
            };

            let record = AuthAttemptRecord {
                s2_cell,
                username: attempt.username,
                ip: attempt.ip,
            };
            self.store_auth_attempt(record).await?;
        }
    }

    async fn get_s2_cell(&self, ip: &str) -> Result<String, Box<dyn Error>> {
        let coords = self.geoip_resolver.get_geoip(ip).await?;
        geoip::s2_cell_from_coords(coords, self.s2_precision)
    }

    async fn store_auth_attempt(&self, attempt: AuthAttemptRecord) -> Result<(), Box<dyn Error>> {
        use influx_db_client::{Precision, Value};
        let point = Point::new("ssh_auth")
            .add_field("username", Value::String(attempt.username))
            .add_field("s2_cell_id", Value::String(attempt.s2_cell))
            .add_field("ip", Value::String(attempt.ip));

        self.influxdb_client
            .write_point(
                point,
                Some(Precision::Seconds),
                self.retention_policy.as_deref(),
            )
            .await?;

        Ok(())
    }
}
