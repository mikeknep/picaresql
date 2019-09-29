use structopt::StructOpt;

use juniper::Config;

fn main() {
    let config = Config::from_args();

    juniper::run(config);
}
