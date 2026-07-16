from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info(f"Truncating dimension table {self.table} before load")
            redshift.run(f"TRUNCATE TABLE {self.table}")
        else:
            self.log.info(f"Appending to dimension table {self.table} (truncate=False)")

        formatted_sql = LoadDimensionOperator.insert_sql.format(self.table, self.sql_query)
        redshift.run(formatted_sql)
        self.log.info(f"Successfully loaded dimension table {self.table}")