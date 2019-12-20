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
    let mut steps = vec![];
    if let SetExpr::Select(select) = &query.body {
        let mut builder_select = create_empty_count_star_select();

        steps.extend(add_from_and_joins(&mut builder_select, select));
        steps.extend(add_selection(&mut builder_select, select));
        steps.extend(add_group_bys(&mut builder_select, select));
        steps.extend(add_having(&mut builder_select, select));
    }
    steps
}

fn create_empty_count_star_select() -> Select {
    Select {
        projection: create_count_star_projection(),
        from: vec![],
        selection: None,
        group_by: vec![],
        having: None,
        distinct: false,
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

fn add_from_and_joins(builder_select: &mut Select, source_select: &Box<Select>) -> Vec<String> {
    let mut clause_steps = vec![];
    for (index, from) in source_select.from.iter().enumerate() {
        let mut builder_from = from.clone();
        builder_from.joins = vec![];
        builder_select.from.push(builder_from.clone());
        clause_steps.append(&mut query_string_from_select(builder_select));

        for join in from.joins.iter() {
            builder_from.joins.push(join.clone());
            builder_select.from[index] = builder_from.clone();
            clause_steps.append(&mut query_string_from_select(builder_select));
        }
    }
    clause_steps
}

fn add_selection(builder_select: &mut Select, source_select: &Box<Select>) -> Vec<String> {
    if let Some(selection) = &source_select.selection {
        builder_select.selection = Some(selection.clone());
        query_string_from_select(builder_select)
    } else {
        vec![]
    }
}

fn add_group_bys(builder_select: &mut Select, source_select: &Box<Select>) -> Vec<String> {
    source_select.group_by.iter().flat_map(|group_by| {
        builder_select.group_by.push(group_by.clone());
        query_string_from_select(builder_select)
    }).collect()
}

fn add_having(builder_select: &mut Select, source_select: &Box<Select>) -> Vec<String> {
    if let Some(having) = &source_select.having {
        builder_select.having = Some(having.clone());
        query_string_from_select(builder_select)
    } else {
        vec![]
    }
}

fn query_string_from_select(builder_select: &Select) -> Vec<String> {
    let query = build_query_with_body(builder_select);
    vec![query.to_string()]
}

fn build_query_with_body(select: &Select) -> Query {
    Query {
        ctes: vec![],
        body: SetExpr::Select(Box::new(select.clone())),
        order_by: vec![],
        limit: None,
        offset: None,
        fetch: None,
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn get_queries(query_analyses: &Vec<QueryAnalysis>) -> Vec<String> {
        query_analyses.iter().map(|qa| qa.query.to_string()).collect()
    }

    fn get_clause_steps(query_analyses: &Vec<QueryAnalysis>) -> Vec<String> {
        query_analyses.iter().flat_map(|qa| qa.clause_steps.clone()).collect()
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
