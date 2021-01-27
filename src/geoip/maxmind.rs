use std::error::Error;
use std::path::Path;

use async_trait::async_trait;
use maxminddb::{geoip2::City, Reader};

use super::{Coords, GeoIPResolver};

pub struct MaxMindDb {
    reader: Reader<Vec<u8>>,
}

impl MaxMindDb {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            reader: maxminddb::Reader::open_readfile(path)?,
        })
    }
}

#[async_trait]
impl GeoIPResolver for MaxMindDb {
    async fn get_geoip(&self, ip: &str) -> Result<Coords, Box<dyn Error>> {
        let city = self.reader.lookup::<City>(ip.parse()?)?;
        let location = city
            .location
            .ok_or_else(|| format!("No location for {}", ip))?;
        Ok(Coords {
            lat: location
                .latitude
                .ok_or_else(|| format!("No latitude for {}", ip))?,
            long: location
                .longitude
                .ok_or_else(|| format!("No longitude for {}", ip))?,
        })
    }
}
