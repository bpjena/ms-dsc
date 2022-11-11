import copy
import json
from jinja2 import Template

SPECIAL_TYPES = ["ARRAY", "JSONB"]
DATABASE = {
    "dev": "redshift",
    "db_prod": "redshift",
    "processed": "redshift",
    "prod": "redshift",
    "production": "mysql",
    "hive_s3": "s3aws"
}
DB_ACCESS_LEVELS = ["readonly", "master"]


class TableSchema(object):
    def __init__(self, tablename, schema, database, columns, metadata):
        """
        :param tablename: tablename of table
        :param schema: schema of table
        :param database: tablename of the db the table lives in
        :param columns: list of length-2 tuples
        :param metadata: dict
        """
        self.tablename = tablename
        self.schema = schema
        self.database = database
        self.columns = columns
        self.metadata = metadata
        self.is_valid(raise_exception=True)

    def __str__(self):
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, schema_dict):
        """Create TableSchema instance from a dictionary

        :param schema_dict: dict
        :return: TableSchema instance
        """
        return cls(
            tablename=schema_dict["tablename"],
            schema=schema_dict["schema"],
            database=schema_dict["database"],
            columns=schema_dict["columns"],
            metadata=schema_dict.get("metadata", dict()),
        )

    @classmethod
    def from_file(cls, dir_path, file_tablename):
        """Create TableSchema instance from a file

        :param json_file_path: str
        :return: TableSchema instance
        """
        with open("{}/{}".format(dir_path, file_tablename)) as f:
            output = f.read()
        if output is not None:
            schema_dict = json.loads(output)
        return cls.from_dict(schema_dict)

    def to_dict(self):
        """
        :return: dict
        """
        return {
            "tablename": self.tablename,
            "schema": self.schema,
            "database": self.database,
            "columns": self.columns,
            "metadata": self.metadata,
        }

    def to_file(self, json_file_path):
        """Dumps schema to a json file

        :param json_file_path:
        :return: None
        """
        with open(json_file_path, mode="w") as f:
            json.dump(self.to_dict(), f, indent=True)

    def get_copy(self, columns, tablename, schema, database):
        """Make a copy with new tablename and schema

        :param columns: list
        :param tablename: str
        :param schema: str
        :param database: str
        :return: TableSchema object
        """
        new_obj = copy.deepcopy(self)
        new_obj.columns = columns
        new_obj.tablename = tablename
        new_obj.schema = schema
        new_obj.database = database
        return new_obj

    def create_redshift_table_sql(
        self,
        schema=None,
        tablename=None,
        columns=None,
        if_not_exists=False
    ):
        # check if the schema is valid
        if schema is None:
            schema = self.schema
        if tablename is None:
            tablename = self.tablename
        if columns is None:
            columns = self.columns
        # print(schema, tablename, columns)
        self.is_valid()
        self.convert_postgres_to_snowflake_type()
        sql = Template(
            """
        CREATE  TABLE 
        {% if if_not_exists %} IF NOT EXISTS{% endif %}
        {{ schema }}.{{ tablename }}
        ({% for c in columns %}
            {{ c[0] }} {{ c[1] }}{% if not loop.last %}, {% endif %}
        {% endfor %}
        )
        """
        ).render(
            if_not_exists=if_not_exists,
            schema=schema,
            tablename=tablename,
            columns=columns
        )
        return sql

    def is_valid(self, raise_exception=True):
        """
        :param raise_exception: bool. Raise if not valid
        :return: bool
        """
        is_valid = True
        reason = None
        # Check columns are not empty
        if not self.columns:
            is_valid = False
            reason = "Table has no columns"
        # Check they have the right shape
        for col in self.columns:
            if len(col) != 2:
                is_valid = False
                reason = "Column length should be 2 (tablename, data type): {}".format(col)
            for datum in col:
                if not isinstance(datum, str):
                    is_valid = False
                    reason = "Column attributes should all be strings or unicode: column={}".format(
                        col
                    )
        # database is valid
        if self.database not in DATABASE:
            is_valid = False
            reason = "DB alias {} not in {}".format(self.database, DATABASE)
        if (not is_valid) and raise_exception:
            raise ValueError(reason)
        return is_valid

    def _column_unsupported(self, column_type):
        return column_type[1].upper() in SPECIAL_TYPES

    def _cast_column(self, column_type):
        if column_type == "time without time zone":
            column_type = "time"
        elif column_type == "date":
            column_type = "TIMESTAMP"
        elif column_type == "time":
            column_type = "time"
        elif column_type == "numeric":
            print("casting column type to FLOAT")
            column_type = "NUMERIC(38, 2)"
        return column_type

    def convert_postgres_to_snowflake_type(self):
        for prod_column in self.columns:
            if self._column_unsupported(prod_column[1]):
                prod_column[1] = "NULL"
            else:
                prod_column[1] = self._cast_column(prod_column[1])
