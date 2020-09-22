use std::error::Error;

use async_trait::async_trait;

pub mod ipstack;

#[async_trait]
pub trait GeoIPResolver {
    async fn get_geoip(&self, ip: &str, precision: usize) -> Result<String, Box<dyn Error>>;
}
