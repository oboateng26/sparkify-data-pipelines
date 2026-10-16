from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadDimensionOperator(BaseOperator):
    """Insert the result of a SELECT statement into a dimension table.

    mode='truncate-insert' empties the table before loading (the default).
    mode='append' keeps the existing rows and adds the new ones.
    """

    ui_color = '#80BD9E'
    valid_modes = ('truncate-insert', 'append')

    def __init__(self,
                 redshift_conn_id='redshift',
                 table='',
                 sql='',
                 mode='truncate-insert',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        if mode not in self.valid_modes:
            raise ValueError('mode must be one of {}, got {}'.format(self.valid_modes, mode))
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.mode = mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.mode == 'truncate-insert':
            self.log.info('Mode truncate-insert: clearing dimension table %s', self.table)
            redshift.run('TRUNCATE TABLE {}'.format(self.table))
        else:
            self.log.info('Mode append: keeping existing rows in %s', self.table)

        self.log.info('Loading dimension table %s', self.table)
        redshift.run('INSERT INTO {} {}'.format(self.table, self.sql))

        records = redshift.get_records('SELECT COUNT(*) FROM {}'.format(self.table))
        self.log.info('Dimension table %s now has %s rows', self.table, records[0][0])
