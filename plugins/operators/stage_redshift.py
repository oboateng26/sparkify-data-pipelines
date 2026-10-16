from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class StageToRedshiftOperator(BaseOperator):
    """Copy JSON files from S3 into a Redshift staging table.

    s3_key is a templated field, so it can include Jinja such as
    "log-data/{{ logical_date.year }}/{{ logical_date.month }}/" to load
    only the files for a run's date and to support backfills.
    """

    ui_color = '#358140'
    template_fields = ('s3_key',)

    copy_sql = """
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        REGION '{region}'
        FORMAT AS JSON '{json_path}'
    """

    def __init__(self,
                 redshift_conn_id='redshift',
                 aws_credentials_id='aws_credentials',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 json_path='auto',
                 region='us-east-1',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region

    def execute(self, context):
        self.log.info('Getting AWS credentials from connection %s', self.aws_credentials_id)
        aws_hook = AwsBaseHook(aws_conn_id=self.aws_credentials_id, client_type='s3')
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Clearing data from staging table %s', self.table)
        redshift.run('DELETE FROM {}'.format(self.table))

        s3_path = 's3://{}/{}'.format(self.s3_bucket, self.s3_key)
        self.log.info('Copying data from %s to %s (JSON option: %s)', s3_path, self.table, self.json_path)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            s3_path=s3_path,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            region=self.region,
            json_path=self.json_path,
        )
        redshift.run(formatted_sql)

        records = redshift.get_records('SELECT COUNT(*) FROM {}'.format(self.table))
        self.log.info('Staging of %s complete: %s rows loaded', self.table, records[0][0])
