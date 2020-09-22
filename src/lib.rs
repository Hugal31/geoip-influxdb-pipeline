pub mod geoip;

use std::error::Error;

use chrono::{DateTime, Utc};
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
    time: DateTime<Utc>,
    success: bool,
    geohash: String,
    username: String,
    ip: String,
}

pub struct Pipeline<G: GeoIPResolver> {
    pub influxdb_client: InfluxDbClient,
    pub geoip_resolver: G,
    pub geohash_precision: usize
}

impl<G: GeoIPResolver> Pipeline<G> {
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

            let geohash = match self.get_geohash(&attempt.ip).await {
                Ok(hash) => hash,
                Err(e) => {
                    eprintln!("Could not get geohash for {}: {}", attempt.ip, e);
                    continue;
                },
            };

            let record = AuthAttemptRecord {
                time: Utc::now(),
                success: false,
                geohash,
                username: attempt.username,
                ip: attempt.ip,
            };
            self.store_auth_attempt(record).await?;
        }
    }

    async fn get_geohash(&self, host: &str) -> Result<String, Box<dyn Error>> {
        self.geoip_resolver.get_geoip(host, self.geohash_precision).await
    }

    async fn store_auth_attempt(&self, attempt: AuthAttemptRecord) -> Result<(), Box<dyn Error>> {
        use influx_db_client::Value;
        let point = Point::new("ssh-auth")
            .add_timestamp(attempt.time.timestamp())
            .add_tag("username", Value::String(attempt.username))
            .add_tag("geohash", Value::String(attempt.geohash))
            .add_field("success", Value::Boolean(attempt.success))
            .add_field("ip", Value::String(attempt.ip));

        self.influxdb_client
            .write_point(point, None, None)
            .await?;

        Ok(())
    }
}
