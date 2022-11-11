import os
import redshift_connector

from python.util.utilities import Util
from python.util.get_file_path import get_relative_file_path
from python.redshift.config.sfdc_transforms_config import config_entries

# Connect to Redshift cluster
rs_con = redshift_connector.connect(
    host=config_entries['redshift_host'],
    database=config_entries['redshift_database'],
    user=os.environ["REDSHIFT_USERNAME"],
    password=os.environ["REDSHIFT_PASSWORD"]
 )
redshift = rs_con.cursor()
LOG = Util.get_logger(__name__)
insert_rows_template = "INSERT INTO {} {} ;"


def _read_file(file_path):
    '''
    read file contents from path input
    :param file_path: str
    :return: str
    '''
    file_path = get_relative_file_path(file_path=file_path, caller_file_path=__file__)
    with open(file_path) as f:
        output = f.read()
    return output


def _delete_rows_or_truncate_table(pattern):
    delete_rows = "DELETE FROM {} WHERE SNAP_DATE = CURRENT_DATE ;"
    truncate_table = "TRUNCATE TABLE {} ;"
    if pattern == "truncate":
        return truncate_table
    elif pattern == "daily_snapshot":
        return delete_rows


def main():
    transform_table_list = config_entries['derived_tables']
    for entry in transform_table_list:
        table = entry['table_nm']
        schema = entry['schema_nm']
        LOG.info("starting.. %s", str(schema)+"."+str(table))
        # delete from table where snap_date = current_date
        cleanup_sql = _delete_rows_or_truncate_table(pattern=entry['load_pattern']).format(schema+"."+table)
        redshift.execute(cleanup_sql)
        redshift.execute("COMMIT ;")
        LOG.info("cleaned.. %s", str(schema)+"."+str(table)+" ["+str(entry['load_pattern'])+"]")
        # insert into table (select)
        select_query_sql = _read_file(entry['transform_sql'])
        insert_into_tbl_sql = insert_rows_template.format(schema+"."+table, select_query_sql)
        redshift.execute(insert_into_tbl_sql)
        redshift.execute("COMMIT ;")
        LOG.info("inserted.. %s", str(schema) + "." + str(table))
    LOG.info("done.")


if __name__ == "__main__":
    main()
    LOG.info("All Done ;-)")