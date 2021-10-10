from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    test_query="SELECT COUNT(*) FROM {table}"
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 tables=[""],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.tables=tables
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        for table in self.tables:
            query = DataQualityOperator.test_query.format(table=table)
            self.log.info('testing {} on {}'.format(query, table))
            result = redshift.get_first(query)[0]
            if result == 0:
                raise  ValueError("failed query {}, no rows returned".format(query))
           