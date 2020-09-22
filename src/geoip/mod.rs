use std::error::Error;

use async_trait::async_trait;

pub mod ipstack;
pub mod maxmind;

#[async_trait]
pub trait GeoIPResolver {
    async fn get_geoip(&self, ip: &str, precision: usize) -> Result<String, Box<dyn Error>>;
}

fn geohash_from_coordinate(latitude: f64, longitude: f64, precision: usize) -> Result<String, Box<dyn Error>> {
    let coordinates = geohash::Coordinate {
        x: longitude,
        y: latitude,
    };
    geohash::encode(coordinates, precision).map_err(Into::into)
}
