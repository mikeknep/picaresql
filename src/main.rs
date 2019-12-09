use structopt::StructOpt;

use picaresql::Config;

fn main() {
    let config = Config::from_args();

    picaresql::run(config);
}
