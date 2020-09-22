use std::error::Error;
use std::path::Path;

use async_trait::async_trait;
use maxminddb::{
    Reader,
    geoip2::City
};

use super::GeoIPResolver;

pub struct MaxMindDb {
    reader: Reader<Vec<u8>>,
}

impl MaxMindDb {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn Error>> {
        Ok(Self { reader: maxminddb::Reader::open_readfile(path)? })
    }
}

#[async_trait]
impl GeoIPResolver for MaxMindDb {
    async fn get_geoip(&self, ip: &str, precision: usize) -> Result<String, Box<dyn Error>> {
        let city = self.reader.lookup::<City>(ip.parse()?)?;
        let location = city.location.ok_or_else(|| format!("No location for {}", ip))?;
        super::geohash_from_coordinate(location.latitude.ok_or_else(|| format!("No latitude for {}", ip))?,
                                       location.longitude.ok_or_else(|| format!("No longitude for {}", ip))?,
                                       precision)
    }
}
