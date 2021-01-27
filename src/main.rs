// From https://medium.com/schkn/geolocating-ssh-hackers-in-real-time-108cbc3b5665

use std::env;
use std::error::Error;

use clap::{App, Arg};
use tokio::net::TcpListener;

use rsyslog_ssh_geoip_pipeline::geoip::GeoIPResolver;
use rsyslog_ssh_geoip_pipeline::{geoip::maxmind::MaxMindDb, Pipeline};
use std::sync::Arc;

// TODO Use option groups for maxmind and ipstack
fn build_options() -> App<'static, 'static> {
    App::new("rsyslog-ssh-geoip-pipeline")
        .version(env!("CARGO_PKG_VERSION"))
        .author("Hugo Laloge <hugo.laloge@gmail.com>")
        .arg(
            Arg::with_name("listen")
                .help("Listen address and port.")
                .short("l")
                .long("listen")
                .takes_value(true)
                .value_name("LISTEN")
                .default_value("127.0.0.1:7070"),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .value_name("PORT")
                .help("The port to listen for log entries")
                .takes_value(true)
                .default_value("7070"),
        )
        .arg(
            Arg::with_name("retention-policy")
                .long("retention-policy")
                .value_name("RETENTION_POLICY")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("precision")
                .long("precision")
                .value_name("PRECISION")
                .help("Precision of the s2_cell")
                .takes_value(true)
                .default_value("3"),
        )
        .arg(
            Arg::with_name("maxmind")
                .long("maxmind")
                .help("Path the to maxmind city database.")
                .takes_value(true)
                .value_name("DATABASE_PATH"),
        )
        .arg(
            Arg::with_name("influx-url")
                .long("influx-url")
                .help("InfluxDB URL")
                .takes_value(true)
                .value_name("INFLUX_URL"),
        )
        .arg(
            Arg::with_name("influx-username")
                .long("influx-username")
                .help("InfluxDB username")
                .takes_value(true)
                .value_name("INFLUX_USERNAME"),
        )
        .arg(
            Arg::with_name("influx-password")
                .long("influx-password")
                .help("InfluxDB password or token")
                .takes_value(true)
                .value_name("INFLUX_PASSWORD"),
        )
        .arg(
            Arg::with_name("influx-database")
                .long("influx-database")
                .help("InfluxDB database")
                .takes_value(true)
                .value_name("DATABASE"),
        )
}

enum GeoIpSource {
    MaxMindDb { database_path: String },
}

struct Arguments {
    pub listen_address: String,
    pub precision: usize,
    pub geoip_source: GeoIpSource,
    pub retention_policy: Option<String>,
    pub influx_address: String,
    pub influx_username: String,
    pub influx_password: String,
    pub influx_database: String,
}

fn parse_arguments() -> Arguments {
    let matches = build_options().get_matches();

    let listen_address = matches.value_of("listen").unwrap().to_owned();
    let precision = clap::value_t!(matches.value_of("precision"), usize).unwrap();
    let retention_policy = matches.value_of("retention-policy").map(|s| s.to_owned());

    let geoip_source = if matches.is_present("maxmind") {
        let database_path = matches.value_of("maxmind").unwrap().to_owned();
        GeoIpSource::MaxMindDb { database_path }
    } else {
        panic!("Missing ipstack or maxmind argument");
    };

    let influx_address = matches
        .value_of("influx-url")
        .map(ToOwned::to_owned)
        .or_else(|| env::var("INFLUX_URL").ok())
        .unwrap();
    let influx_username = matches
        .value_of("influx-username")
        .map(ToOwned::to_owned)
        .or_else(|| env::var("INFLUX_USERNAME").ok())
        .unwrap();
    let influx_password = matches
        .value_of("influx-password")
        .map(ToOwned::to_owned)
        .or_else(|| env::var("INFLUX_PASSWORD").ok())
        .unwrap();
    let influx_database = matches
        .value_of("influx-database")
        .map(ToOwned::to_owned)
        .or_else(|| env::var("INFLUX_DATABASE").ok())
        .unwrap();

    Arguments {
        listen_address,
        precision,
        geoip_source,
        retention_policy,
        influx_address,
        influx_username,
        influx_password,
        influx_database,
    }
}

fn create_geoip_source(
    source: GeoIpSource,
) -> Result<Box<dyn GeoIPResolver + Send + Sync + 'static>, Box<dyn Error>> {
    Ok(match source {
        GeoIpSource::MaxMindDb { database_path } => Box::new(MaxMindDb::new(database_path)?),
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_arguments();
    let mut listener = TcpListener::bind(args.listen_address).await?;

    let pipeline = Arc::new(Pipeline {
        influxdb_client: influx_db_client::Client::new(
            args.influx_address.parse()?,
            args.influx_database.clone(),
        )
        .set_authentication(args.influx_username, args.influx_password),
        geoip_resolver: create_geoip_source(args.geoip_source)?,
        s2_precision: args.precision,
        retention_policy: args.retention_policy,
    });

    loop {
        let (socket, _) = listener.accept().await?;
        let pipeline_cp = pipeline.clone();

        tokio::spawn(async move {
            if let Err(e) = pipeline_cp.handle_client(socket).await {
                eprintln!("Error: {}", e);
            }
        });
    }
}
