from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 conn_id = "",
                 insert_query="",
                 truncate="",
                 table="",
                 append_data="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.conn_id = conn_id,
        self.insert_query = insert_query,
        self.table = table,
        self.truncate = truncate,
        self.append_data = append_data

    def execute(self, context):
        # self.log.info('LoadDimensionOperator not implemented yet')
        self.log.info('Starting load_dimension')
        hook = PostgresHook(postgres_conn_id=conn_id)
        
        if append_data == True:
            redshift.run(f"INSERT INTO {table} {insert_query}")
        else:
            redshift.run(f"DELETE FROM {table}")

            redshift.run(f"INSERT INTO {table} {insert_query}")
            
        self.log.info('Finished load_dimension')