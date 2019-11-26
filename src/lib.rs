use std::fs;
use std::io;
use structopt::StructOpt;

extern crate sqlparser;
use sqlparser::ast::{Statement, Query};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[derive(StructOpt, Debug)]
#[structopt(name = "picaresql", about = "Debug your SQL")]
pub struct Config {
    #[structopt(long, help = "Should be in the form 'postgres://user:password@host:port/db_name'")]
    pub connection_string: String,

    #[structopt(name = "sql file")]
    pub sql_file: String,
}

impl Config {
    pub fn sql(&self) -> Result<String, io::Error> {
        fs::read_to_string(&self.sql_file)
    }
}

#[derive(Debug)]
struct Analysis {
    pub query_analyses: Vec<QueryAnalysis>,
}

#[derive(Debug)]
struct QueryAnalysis {
    pub query: String,
}


pub fn run(config: Config) {
    let sql = config.sql().unwrap();
    let analysis = analyze(&sql);
    println!("{:?}", analysis);
}

fn analyze(sql: &str) -> Analysis {
    let query_analyses = analyze_queries(&sql);
    Analysis { query_analyses }
}

fn analyze_queries(sql: &str) -> Vec<QueryAnalysis> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql.to_string()).unwrap();

    ast.iter().filter_map(|statement| {
        match statement {
            Statement::Query(q) => Some(analyze_query(q)),
            _ => None
        }
    }).collect()
}

fn analyze_query(query: &Query) -> QueryAnalysis {
    QueryAnalysis { query: query.to_string() }
}



#[cfg(test)]
mod tests {
    use super::*;

    fn get_queries(query_analyses: &Vec<QueryAnalysis>) -> Vec<String> {
        query_analyses.iter().map(|qa| qa.query.to_string()).collect()
    }

    #[test]
    fn creates_one_query_analysis_for_simple_query() {
        let sql = "SELECT * FROM table_1";

        let query_analyses = analyze_queries(&sql);
        let queries = get_queries(&query_analyses);

        assert_eq!(vec![sql], queries);
    }

    #[test]
    fn creates_n_query_analyses_for_n_queries() {
        let sql = "SELECT * FROM table_1; SELECT * FROM table_2";

        let expected_queries = vec![
            "SELECT * FROM table_1",
            "SELECT * FROM table_2",
        ];

        let query_analyses = analyze_queries(&sql);
        let queries = get_queries(&query_analyses);

        assert_eq!(expected_queries, queries);
    }

    #[test]
    fn does_not_treat_non_query_statements_as_queries() {
        let sql = "DROP TABLE table_1";

        let query_analyses = analyze_queries(&sql);

        assert_eq!(0, query_analyses.len());
    }
}
