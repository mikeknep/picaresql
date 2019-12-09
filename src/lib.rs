use std::fs;
use std::io;
use structopt::StructOpt;

extern crate sqlparser;
use sqlparser::ast::{Statement, Query, SetExpr, Function, ObjectName, Expr, SelectItem, Select};
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

#[derive(Debug, Clone)]
struct QueryAnalysis {
    pub query: String,
    pub clause_steps: Vec<String>,
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
    QueryAnalysis {
        query: query.to_string(),
        clause_steps: clause_steps_for_query(query),
    }
}

fn clause_steps_for_query(query: &Query) -> Vec<String> {
    if let SetExpr::Select(select) = &query.body {
        let mut clause_steps: Vec<String> = Vec::new();

        let mut builder_query = query.clone();
        let mut builder_select = select.clone();

        builder_select.projection = create_count_star_projection();
        builder_select.from = vec![];
        builder_select.selection = None;
        builder_select.group_by = vec![];
        builder_select.having = None;

        for (index, from) in select.from.iter().enumerate() {
            let mut builder_from = from.clone();
            builder_from.joins = vec![];
            builder_select.from.push(builder_from.clone());
            take_snapshot(&mut clause_steps, &mut builder_query, &builder_select);

            for join in from.joins.iter() {
                builder_from.joins.push(join.clone());
                builder_select.from[index] = builder_from.clone();
                take_snapshot(&mut clause_steps, &mut builder_query, &builder_select);
            }
        }

        if let Some(selection) = &select.selection {
            builder_select.selection = Some(selection.clone());
            take_snapshot(&mut clause_steps, &mut builder_query, &builder_select);
        }

        for group_by in select.group_by.iter() {
            builder_select.group_by.push(group_by.clone());
            take_snapshot(&mut clause_steps, &mut builder_query, &builder_select);
        }

        if let Some(having) = &select.having {
            builder_select.having = Some(having.clone());
            take_snapshot(&mut clause_steps, &mut builder_query, &builder_select);
        }

        clause_steps
    } else {
        vec![]
    }
}

fn create_count_star_projection() -> Vec<SelectItem> {
    let count = Function {
        name: ObjectName(vec![String::from("COUNT")]),
        args: vec![Expr::Wildcard],
        over: None,
        distinct: false,
    };
    vec![SelectItem::UnnamedExpr(Expr::Function(count))]
}

fn take_snapshot(clause_steps: &mut Vec<String>, builder_query: &mut Query, builder_select: &Box<Select>) {
    builder_query.body = SetExpr::Select(builder_select.clone());
    clause_steps.push(builder_query.clone().to_string());
}



#[cfg(test)]
mod tests {
    use super::*;

    fn get_queries(query_analyses: &Vec<QueryAnalysis>) -> Vec<String> {
        query_analyses.iter().map(|qa| qa.query.to_string()).collect()
    }

    fn get_clause_steps(query_analyses: &Vec<QueryAnalysis>) -> Vec<String> {
        query_analyses.iter().flat_map(|qa| qa.clone().clause_steps).collect()
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

    #[test]
    fn decomposes_from_with_explicitly_joined_table_to_counting_clause_steps() {
        let sql = "SELECT * FROM table_1 JOIN table_2 ON true";

        let expected_clause_steps = vec![
            "SELECT COUNT(*) FROM table_1",
            "SELECT COUNT(*) FROM table_1 JOIN table_2 ON true",
        ];

        let query_analyses = analyze_queries(&sql);
        let clause_steps = get_clause_steps(&query_analyses);

        assert_eq!(expected_clause_steps, clause_steps);
    }

    #[test]
    fn decomposes_from_with_multiple_explicitly_joined_tables_to_counting_clause_steps() {
        let sql = "SELECT * FROM table_1 JOIN table_2 ON true LEFT JOIN table_3 ON table_3.x = table_2.x";

        let expected_clause_steps = vec![
            "SELECT COUNT(*) FROM table_1",
            "SELECT COUNT(*) FROM table_1 JOIN table_2 ON true",
            "SELECT COUNT(*) FROM table_1 JOIN table_2 ON true LEFT JOIN table_3 ON table_3.x = table_2.x",
        ];

        let query_analyses = analyze_queries(&sql);
        let clause_steps = get_clause_steps(&query_analyses);

        assert_eq!(expected_clause_steps, clause_steps);
    }

    #[test]
    fn decomposes_from_with_comma_separated_table_to_counting_clause_steps() {
        let sql = "SELECT * FROM table_1, table_2";

        let expected_clause_steps = vec![
            "SELECT COUNT(*) FROM table_1",
            "SELECT COUNT(*) FROM table_1, table_2",
        ];

        let query_analyses = analyze_queries(&sql);
        let clause_steps = get_clause_steps(&query_analyses);

        assert_eq!(expected_clause_steps, clause_steps);
    }

    #[test]
    fn decomposes_from_with_join_and_where_to_counting_clause_steps() {
        let sql = "SELECT * FROM table_1 JOIN table_2 ON true WHERE x = 1";

        let expected_clause_steps = vec![
            "SELECT COUNT(*) FROM table_1",
            "SELECT COUNT(*) FROM table_1 JOIN table_2 ON true",
            "SELECT COUNT(*) FROM table_1 JOIN table_2 ON true WHERE x = 1",
        ];

        let query_analyses = analyze_queries(&sql);
        let clause_steps = get_clause_steps(&query_analyses);

        assert_eq!(expected_clause_steps, clause_steps);
    }

    #[test]
    fn decomposes_group_by_to_counting_clause_steps() {
        let sql = "SELECT * FROM table_1 GROUP BY x";

        let expected_clause_steps = vec![
            "SELECT COUNT(*) FROM table_1",
            "SELECT COUNT(*) FROM table_1 GROUP BY x",
        ];

        let query_analyses = analyze_queries(&sql);
        let clause_steps = get_clause_steps(&query_analyses);

        assert_eq!(expected_clause_steps, clause_steps);
    }

    #[test]
    fn decomposes_multiple_group_bys_to_counting_clause_steps() {
        let sql = "SELECT * FROM table_1 GROUP BY x, y";

        let expected_clause_steps = vec![
            "SELECT COUNT(*) FROM table_1",
            "SELECT COUNT(*) FROM table_1 GROUP BY x",
            "SELECT COUNT(*) FROM table_1 GROUP BY x, y",
        ];

        let query_analyses = analyze_queries(&sql);
        let clause_steps = get_clause_steps(&query_analyses);

        assert_eq!(expected_clause_steps, clause_steps);
    }

    #[test]
    fn decomposes_group_by_with_having_to_counting_clause_steps() {
        let sql = "SELECT * FROM table_1 GROUP BY x HAVING COUNT(*) > 1";

        let expected_clause_steps = vec![
            "SELECT COUNT(*) FROM table_1",
            "SELECT COUNT(*) FROM table_1 GROUP BY x",
            "SELECT COUNT(*) FROM table_1 GROUP BY x HAVING COUNT(*) > 1",
        ];

        let query_analyses = analyze_queries(&sql);
        let clause_steps = get_clause_steps(&query_analyses);

        assert_eq!(expected_clause_steps, clause_steps);
    }
}
