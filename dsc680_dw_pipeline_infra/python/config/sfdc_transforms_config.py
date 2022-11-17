config_entries = {
    "redshift_host": "redshift-cluster-1.c6audql62kaf.us-west-2.redshift.amazonaws.com",
    "redshift_port": "5439",
    "redshift_database": "db_prod",
    "derived_tables": [
        {
            "dataset": "open_pipe_coverage",
            "table_nm": "open_pipe_coverage",
            "schema_nm": "sfdc",
            "transform_sql": "queries/sfdc_open_pipe_coverage.sql",
            "load_pattern": "truncate"
        },
    ]
}