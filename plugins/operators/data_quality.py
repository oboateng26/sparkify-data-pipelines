import operator

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataQualityOperator(BaseOperator):
    """Run SQL test cases against Redshift and fail if any result is wrong.

    Each test case is a dict passed in from the DAG:
        {'check_sql': 'SELECT COUNT(*) FROM users WHERE userid IS NULL',
         'expected_result': 0,
         'comparison': '=='}
    comparison is optional and defaults to '=='.
    """

    ui_color = '#89DA59'
    comparisons = {
        '==': operator.eq,
        '!=': operator.ne,
        '>': operator.gt,
        '>=': operator.ge,
        '<': operator.lt,
        '<=': operator.le,
    }

    def __init__(self,
                 redshift_conn_id='redshift',
                 dq_checks=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks or []

    def execute(self, context):
        if not self.dq_checks:
            raise ValueError('No data quality checks were provided')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        failures = []

        for number, check in enumerate(self.dq_checks, start=1):
            sql = check['check_sql']
            expected = check['expected_result']
            comparison = check.get('comparison', '==')
            if comparison not in self.comparisons:
                raise ValueError('Unknown comparison {} in check {}'.format(comparison, number))

            self.log.info('Check %s: %s (expect result %s %s)', number, sql, comparison, expected)
            records = redshift.get_records(sql)
            if not records or not records[0]:
                failures.append('Check {} returned no results: {}'.format(number, sql))
                continue

            actual = records[0][0]
            if self.comparisons[comparison](actual, expected):
                self.log.info('Check %s passed: result was %s', number, actual)
            else:
                failures.append('Check {} failed: {} returned {}, expected {} {}'.format(
                    number, sql, actual, comparison, expected))

        if failures:
            for failure in failures:
                self.log.error(failure)
            raise ValueError('Data quality checks failed: {} of {}'.format(len(failures), len(self.dq_checks)))

        self.log.info('All %s data quality checks passed', len(self.dq_checks))
