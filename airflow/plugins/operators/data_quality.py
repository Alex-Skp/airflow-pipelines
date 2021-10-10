from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 tests=[""],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        for test in self.tests:
            query = test.get('test')
            self.log.info('testing {}'.format(query))
            result = redshift.get_first(query)[0]
            if result != test.get('exp_result'):
                raise  ValueError("failed test query {}".format(query))
           