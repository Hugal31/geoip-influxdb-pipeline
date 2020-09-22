use std::error::Error;

use async_trait::async_trait;

use reqwest::{Client as HttpClient, Method};
use serde::{Deserialize, Serialize};
use crate::geoip::GeoIPResolver;

#[derive(Clone, Serialize, Deserialize)]
pub struct IpStackRecord {
    pub ip: String,
    #[serde(rename="type")]
    pub typ: String,
    pub continent_code: String,
    pub continent_name: String,
    pub latitude: f64,
    pub longitude: f64,
}

pub struct IpStackClient {
    access_key: String,
    client: HttpClient,
}

impl IpStackClient {
    pub fn new<S: Into<String>>(access_key: S) -> Self {
        IpStackClient {
            access_key: access_key.into(),
            client: HttpClient::new(),
        }
    }

    pub async fn request(&self, ip: &str) -> Result<IpStackRecord, Box<dyn Error>> {
        let request = self
            .client
            .request(Method::GET, &format!("http://api.ipstack.com/{}", ip))
            .query(&[("access_key", &self.access_key)])
            .build()?;
        let record = self.client.execute(request).await?.json().await?;

        Ok(record)
    }
}

#[async_trait]
impl GeoIPResolver for IpStackClient {
    async fn get_geoip(&self, ip: &str, precision: usize) -> Result<String, Box<dyn Error>> {
        let geoip_record = self.request(ip).await?;
        let coordinates = geohash::Coordinate {
            x: geoip_record.longitude,
            y: geoip_record.latitude,
        };
        geohash::encode(coordinates, precision).map_err(Into::into)
    }
}
