// From https://medium.com/schkn/geolocating-ssh-hackers-in-real-time-108cbc3b5665

use std::error::Error;

use clap::{App, Arg};
use tokio::net::TcpListener;

use rsyslog_ssh_geoip_pipeline::{
    geoip::{
        ipstack::IpStackClient,
        maxmind::MaxMindDb,
    },
    Pipeline
};
use std::sync::Arc;
use rsyslog_ssh_geoip_pipeline::geoip::GeoIPResolver;

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
        .arg(Arg::with_name("retention-policy")
             .long("retention-policy")
             .value_name("RETENTION_POLICY")
             .takes_value(true))
        .arg(Arg::with_name("precision")
            .long("precision")
            .value_name("PRECISION")
            .help("Precision of the geohash")
            .takes_value(true)
            .default_value("3"))
        .arg(Arg::with_name("ipstack")
            .long("ipstack")
            .help("Use ipstack to get the GEOIP info")
            .takes_value(true)
            .value_name("ACCESS_KEY")
            .conflicts_with("maxmind"))
        .arg(Arg::with_name("maxmind")
            .long("maxmind")
            .help("Path the to maxmind city database.")
            .takes_value(true)
            .value_name("DATABASE_PATH")
            .conflicts_with("ipstack"))
}

enum GeoIpSource {
    IpStack {
        access_key: String,
    },
    MaxMindDb {
        database_path: String,
    }
}

struct Arguments {
    pub listen_address: String,
    pub precision: usize,
    pub geoip_source: GeoIpSource,
    pub retention_policy: Option<String>,
}

fn parse_arguments() -> Arguments {
    let matches = build_options().get_matches();

    let listen_address = matches.value_of("listen").unwrap().to_owned();
    let precision = clap::value_t!(matches.value_of("precision"), usize).unwrap();
    let retention_policy = matches.value_of("retention-policy").map(|s| s.to_owned());

    let geoip_source = if matches.is_present("ipstack") {
        let access_key = matches.value_of("ipstack").unwrap().to_owned();
        GeoIpSource::IpStack { access_key }
    } else if matches.is_present("maxmind") {
        let database_path = matches.value_of("maxmind").unwrap().to_owned();
        GeoIpSource::MaxMindDb { database_path }
    } else {
        panic!("Missing ipstack or maxmind argument");
    };

    Arguments { listen_address, precision, geoip_source, retention_policy }
}

fn create_geoip_source(source: GeoIpSource) -> Result<Box<dyn GeoIPResolver + Send + Sync + 'static>, Box<dyn Error>> {
    Ok(match source {
        GeoIpSource::IpStack { access_key } => Box::new(IpStackClient::new(access_key)),
        GeoIpSource::MaxMindDb { database_path } => Box::new(MaxMindDb::new(database_path)?),
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_arguments();
    let mut listener = TcpListener::bind(args.listen_address).await?;

    let pipeline = Arc::new(Pipeline {
        influxdb_client: influx_db_client::Client::new("http://localhost:8086".parse()?, "monitoring"),
        geoip_resolver: create_geoip_source(args.geoip_source)?,
        geohash_precision: args.precision,
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
