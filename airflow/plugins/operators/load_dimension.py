from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    load_dim_sql="""
    TRUNCATE TABLE {final_table};
    
    INSERT INTO {final_table}
    (
    {query}
    );
    """
    
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 query="",
                 final_table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.final_table = final_table

    def execute(self, context):
        self.log.info('Loading dimension table: {}'.format(self.final_table))
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        final_query = LoadDimensionOperator.load_dim_sql.format(
            final_table=self.final_table,
            query=self.query
        )
        redshift.run(final_query)
