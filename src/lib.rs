use std::fs;
use std::io;
use structopt::StructOpt;

extern crate sqlparser;
use sqlparser::ast::{Statement, Query, SetExpr, Function, ObjectName, Expr, SelectItem, Select, TableWithJoins, Values};
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
    pub insert_analyses: Vec<InsertAnalysis>,
}

impl Analysis {
    fn new() -> Analysis {
        Analysis {
            query_analyses: vec![],
            insert_analyses: vec![],
        }
    }

    fn add_query_analysis(mut self, query_analysis: QueryAnalysis) -> Analysis {
        self.query_analyses.push(query_analysis);
        self
    }

    fn add_insert_analysis(mut self, insert_analysis: InsertAnalysis) -> Analysis {
        self.insert_analyses.push(insert_analysis);
        self
    }
}

#[derive(Debug, Clone)]
struct QueryAnalysis {
    pub query: String,
    pub clause_steps: Vec<String>,
}

#[derive(Debug, Clone)]
struct InsertAnalysis {
    pub insert_statement: String,
    pub target_table_initial_count: String,
    pub payload_count: String,
}


pub fn run(config: Config) {
    let sql = config.sql().unwrap();
    let analysis = analyze(&sql);
    println!("{:?}", analysis);
}

fn analyze(sql: &str) -> Analysis {
    let ast = get_ast_for_sql(sql);
    ast.iter().fold(Analysis::new(), |analysis, statement| {
        match statement {
            Statement::Query(q) => analysis.add_query_analysis(analyze_query(q)),
            Statement::Insert { table_name, columns: _, source } => analysis.add_insert_analysis(analyze_insert(table_name, source, statement)),
            _ => analysis
        }
    })
}

fn get_ast_for_sql(sql: &str) -> Vec<Statement> {
    let dialect = GenericDialect {};
    Parser::parse_sql(&dialect, sql.to_string()).unwrap()
}

fn analyze_insert(table_name: &ObjectName, source: &Query, full_statement: &Statement) -> InsertAnalysis {
    let target_table_initial_count = format!("SELECT COUNT(*) FROM {}", table_name);
    let payload_count = get_payload_count_query(source);

    InsertAnalysis {
        insert_statement: full_statement.to_string(),
        target_table_initial_count,
        payload_count,
    }
}

fn get_payload_count_query(query: &Query) -> String {
    match &query.body {
        SetExpr::Select(select) => transform_select_projection_to_count(*select.clone()),
        SetExpr::Values(values) => get_values_count_query(values),
        _ => panic!("What are you trying to INSERT if not a SELECT or VALUES?")
    }
}

fn transform_select_projection_to_count(mut select: Select) -> String {
    select.projection = create_count_star_projection();
    select.to_string()
}

fn get_values_count_query(values: &Values) -> String {
    format!("SELECT {}", values.0.len())
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
        let mut builder_from = TableWithJoins { relation: from.relation.clone(), joins: vec![] };
        builder_select.from.push(builder_from.clone());
        clause_steps.extend(query_string_from_select(builder_select));

        for join in from.joins.iter() {
            builder_from.joins.push(join.clone());
            builder_select.from[index] = builder_from.clone();
            clause_steps.extend(query_string_from_select(builder_select));
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

    fn get_queries(analysis: &Analysis) -> Vec<String> {
        analysis.query_analyses.iter().map(|qa| qa.query.to_string()).collect()
    }

    fn get_clause_steps(analysis: &Analysis) -> Vec<String> {
        analysis.query_analyses.iter().flat_map(|qa| qa.clause_steps.clone()).collect()
    }

    #[test]
    fn creates_one_query_analysis_for_simple_query() {
        let sql = "SELECT * FROM table_1";

        let analysis = analyze(&sql);
        let queries = get_queries(&analysis);

        assert_eq!(vec![sql], queries);
    }

    #[test]
    fn creates_n_query_analyses_for_n_queries() {
        let sql = "SELECT * FROM table_1; SELECT * FROM table_2";

        let expected_queries = vec![
            "SELECT * FROM table_1",
            "SELECT * FROM table_2",
        ];

        let analysis = analyze(&sql);
        let queries = get_queries(&analysis);

        assert_eq!(expected_queries, queries);
    }

    #[test]
    fn does_not_treat_non_query_statements_as_queries() {
        let sql = "DROP TABLE table_1";

        let analysis = analyze(&sql);

        assert_eq!(0, analysis.query_analyses.len());
    }

    #[test]
    fn decomposes_from_with_explicitly_joined_table_to_counting_clause_steps() {
        let sql = "SELECT * FROM table_1 JOIN table_2 ON true";

        let expected_clause_steps = vec![
            "SELECT COUNT(*) FROM table_1",
            "SELECT COUNT(*) FROM table_1 JOIN table_2 ON true",
        ];

        let analysis = analyze(&sql);
        let clause_steps = get_clause_steps(&analysis);

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

        let analysis = analyze(&sql);
        let clause_steps = get_clause_steps(&analysis);

        assert_eq!(expected_clause_steps, clause_steps);
    }

    #[test]
    fn decomposes_from_with_comma_separated_table_to_counting_clause_steps() {
        let sql = "SELECT * FROM table_1, table_2";

        let expected_clause_steps = vec![
            "SELECT COUNT(*) FROM table_1",
            "SELECT COUNT(*) FROM table_1, table_2",
        ];

        let analysis = analyze(&sql);
        let clause_steps = get_clause_steps(&analysis);

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

        let analysis = analyze(&sql);
        let clause_steps = get_clause_steps(&analysis);

        assert_eq!(expected_clause_steps, clause_steps);
    }

    #[test]
    fn decomposes_group_by_to_counting_clause_steps() {
        let sql = "SELECT * FROM table_1 GROUP BY x";

        let expected_clause_steps = vec![
            "SELECT COUNT(*) FROM table_1",
            "SELECT COUNT(*) FROM table_1 GROUP BY x",
        ];

        let analysis = analyze(&sql);
        let clause_steps = get_clause_steps(&analysis);

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

        let analysis = analyze(&sql);
        let clause_steps = get_clause_steps(&analysis);

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

        let analysis = analyze(&sql);
        let clause_steps = get_clause_steps(&analysis);

        assert_eq!(expected_clause_steps, clause_steps);
    }

    #[test]
    fn checks_the_count_of_the_target_table_of_an_insert_statement() {
        let sql = "INSERT INTO table_1 SELECT * FROM table_2";

        let expected_target_table_initial_count_queries = vec!["SELECT COUNT(*) FROM table_1"];

        let analysis = analyze(&sql);
        let target_table_initial_count_queries: Vec<String> = analysis.insert_analyses.iter().map(|ia| ia.target_table_initial_count.to_string()).collect();

        assert_eq!(expected_target_table_initial_count_queries, target_table_initial_count_queries)
    }

    #[test]
    fn checks_the_count_of_the_payload_of_an_insert_statement_using_select() {
        let sql = "INSERT INTO table_1 SELECT * FROM table_2";

        let expected_payload_count_queries = vec!["SELECT COUNT(*) FROM table_2"];

        let analysis = analyze(&sql);
        let payload_count_queries: Vec<String> = analysis.insert_analyses.iter().map(|ia| ia.payload_count.to_string()).collect();

        assert_eq!(expected_payload_count_queries, payload_count_queries)
    }

    #[test]
    fn checks_the_count_of_the_payload_of_an_insert_statement_using_values() {
        let sql = "INSERT INTO table_1 (a) VALUES (1), (2)";

        let expected_payload_count_queries = vec!["SELECT 2"];

        let analysis = analyze(&sql);
        let payload_count_queries: Vec<String> = analysis.insert_analyses.iter().map(|ia| ia.payload_count.to_string()).collect();

        assert_eq!(expected_payload_count_queries, payload_count_queries)
    }
}
