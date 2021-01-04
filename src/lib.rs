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

pub struct Pipeline {
    pub influxdb_client: InfluxDbClient,
    pub geoip_resolver: Box<dyn GeoIPResolver + Sync + Send + 'static>,
    pub geohash_precision: usize
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

    pub async fn fill_database(&self) -> Result<(), Box<dyn Error>> {
        let query = self.influxdb_client.query("select ip from \"ssh-auth\" WHERE geohash = ''", None).await?;
        if let Some(nodes) = query {
            for node in nodes {
                if let Some(series) = node.series {
                    for serie in series {
                        let ip_idx = serie.columns.iter().position(|s| s == "ip").ok_or(Box::new(format!("Could not find ip field")))?;

                        for record in serie.values {
                            let ip = record[ip_idx].as_str().ok_or(Box::new(format!("Could not get String IP value")))?;
                            let geohash = self.get_geohash(ip);

                            self.influxdb_client.write
                        }
                    }
                }
            }
        }

        Ok(());
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
