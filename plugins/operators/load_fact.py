from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadFactOperator(BaseOperator):
    """Insert the result of a SELECT statement into a fact table.

    Fact tables can be very large, so this operator only appends rows.
    """

    ui_color = '#F98866'

    def __init__(self,
                 redshift_conn_id='redshift',
                 table='',
                 sql='',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Appending rows to fact table %s', self.table)
        redshift.run('INSERT INTO {} {}'.format(self.table, self.sql))

        records = redshift.get_records('SELECT COUNT(*) FROM {}'.format(self.table))
        self.log.info('Fact table %s now has %s rows', self.table, records[0][0])
