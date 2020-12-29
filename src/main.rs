// From https://medium.com/schkn/geolocating-ssh-hackers-in-real-time-108cbc3b5665

use std::error::Error;

use clap::{App, Arg};
use tokio::net::TcpListener;


use rsyslog_ssh_geoip_pipeline::{geoip::ipstack::IpStackClient, Pipeline};
use std::sync::Arc;

fn build_options() -> App<'static, 'static> {
    App::new("rsyslog-ssh-geoip-pipeline")
        .version(env!("CARGO_PKG_VERSION"))
        .author("Hugo Laloge <hugo.laloge@gmail.com>")
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .value_name("HOST")
                .takes_value(true)
                .default_value("127.0.0.1"),
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
        .arg(Arg::with_name("ipstack-api-key")
            .long("ipstack-api-key")
            .value_name("ACCESS_KEY")
            .takes_value(true)
            .required(true))
}

struct Arguments {
    pub host: String,
    pub port: String,
    pub precision: usize,
    pub ipstack_key: String,
    pub retention_policy: Option<String>,
}

fn parse_arguments() -> Arguments {
    let matches = build_options().get_matches();

    let host = matches.value_of("host").unwrap().to_owned();
    let port = matches.value_of("port").unwrap().to_owned();
    let precision = clap::value_t!(matches.value_of("precision"), usize).unwrap();
    let ipstack_key = matches.value_of("ipstack-api-key").unwrap().to_owned();
    let retention_policy = matches.value_of("retention-policy").map(|s| s.to_owned());

    Arguments { host, port, precision, ipstack_key, retention_policy }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_arguments();
    let mut listener = TcpListener::bind(format!("{}:{}", args.host, args.port)).await?;

    let pipeline = Arc::new(Pipeline {
        influxdb_client: influx_db_client::Client::new("http://localhost:8086".parse()?, "monitoring"),
        geoip_resolver: IpStackClient::new(args.ipstack_key),
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
