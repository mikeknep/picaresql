use std::fs;
use std::io;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "juniper", about = "Debug your SQL")]
pub struct Config {
    #[structopt(long, help = "Should be in the form 'postgres://user:password@host:port/db_name'")]
    pub connection_string: String,

    #[structopt(name = "statement file")]
    pub statement_file: String,
}

impl Config {
    pub fn statement(&self) -> Result<String, io::Error> {
        fs::read_to_string(&self.statement_file)
    }
}

pub fn run(config: Config) {
    println!("{:?}", config);
    println!("{}", config.statement().unwrap());
}
