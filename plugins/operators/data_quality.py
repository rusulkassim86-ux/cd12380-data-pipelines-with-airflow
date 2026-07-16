from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks or []

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.dq_checks:
            raise AirflowException("DataQualityOperator was given no checks to run")

        for check in self.dq_checks:
            sql = check.get('check_sql')
            expected_result = check.get('expected_result')

            self.log.info(f"Running data quality check: {sql}")
            records = redshift.get_records(sql)
            actual_result = records[0][0]

            if actual_result != expected_result:
                raise AirflowException(
                    f"Data quality check failed. '{sql}' returned "
                    f"{actual_result}, expected {expected_result}"
                )

            self.log.info(f"Data quality check passed: {sql} -> {actual_result}")

        self.log.info("All data quality checks passed")