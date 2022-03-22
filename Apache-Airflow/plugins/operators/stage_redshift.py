from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    staging_events_copy = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json '{}';
     """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 table="",
                 conn_id="",
                 s3_bucket="",
                 s3_key="",
                 aws_credentials_id="",
                 file_type="",
               
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.conn_id = conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.file_type = file_type
        

    def execute(self, context):
        # self.log.info('StageToRedshiftOperator not implemented yet')
        self.log.info('Starting stage_redshift')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id =self.conn_id)
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        formatted_sql = self.staging_events_copy.format(
            table_name = self.table,
            s3_path = self.s3_path,
            access_key = credentials.access_key,
            secret_key = credentials.secret_key,
            file_type = self.file_type)
        
        redshift.run(formatted_sql)
        self.log.info('Finished stage_redshift')