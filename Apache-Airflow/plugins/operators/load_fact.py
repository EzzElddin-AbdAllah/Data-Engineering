from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 conn_id = "",
                 insert_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.conn_id = conn_id,
        self.insert_query = insert_query

    def execute(self, context):
        # self.log.info('LoadFactOperator not implemented yet')
        self.log.info('Starting load_fact')
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        hook.run(self.insert_query)
        self.log.info('Finished load_fact')
