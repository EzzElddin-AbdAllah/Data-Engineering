from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 conn_id = "",
                 tables=[],
                 test_queries=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.tables = tables
        self.conn_id = conn_id

    def execute(self, context):
        # self.log.info('DataQualityOperator not implemented yet')
        self.log.info('Starting data_quality')
        redshift_postgres = PostgresHook(postgres_conn_id =self.conn_id)
        results = []
        for query in test_queries:
            results.append(redshift_postgres.get_records(self.query))
            
        self.log.info(results)
        for result in results:
            if result == 0:
                raise ValueError("Data quality check failed")
            else:
                self.log.info("Data quality check passed.")
                
        self.log.info('Finished data_quality')
        