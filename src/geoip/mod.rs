use std::error::Error;

use async_trait::async_trait;

pub mod maxmind;

pub struct Coords {
    lat: f64,
    long: f64,
}

#[async_trait]
pub trait GeoIPResolver {
    async fn get_geoip(&self, ip: &str) -> Result<Coords, Box<dyn Error>>;
}

pub fn geohash_from_coords(coords: Coords, precision: usize) -> Result<String, Box<dyn Error>> {
    let coordinates = geohash::Coordinate {
        x: coords.long,
        y: coords.lat,
    };
    geohash::encode(coordinates, precision).map_err(Into::into)
}

pub fn s2_cell_from_coords(coords: Coords, precision: usize) -> Result<String, Box<dyn Error>> {
    use s2::{cell::Cell, latlng::LatLng, s1::Deg};

    let precision = precision as u64;
    let latlng = LatLng::new(Deg(coords.lat).into(), Deg(coords.long).into());
    let mut cell = Cell::from(latlng);
    if cell.level() as u64 > precision {
        cell = Cell::from(cell.id.parent(precision));
    }

    Ok(cell.id.to_token())
}
