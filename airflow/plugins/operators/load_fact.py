from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    upsert_sql ="""
    DROP TABLE IF EXISTS temp_table;
    
    CREATE TABLE temp_table AS(
    {query}
    );
    
    DELETE FROM {final_table}
    WHERE {primary_key} IN (SELECT {primary_key} FROM temp_table);
    
    INSERT INTO {final_table}
    (SELECT * FROM temp_table);
    
    """
    
    append_sql="""
    DROP TABLE IF EXISTS temp_table;
    
    CREATE TABLE temp_table AS(
    {query}
    );
    
    INSERT INTO {final_table}
    (SELECT * FROM temp_table);
    """
    

    @apply_defaults
    def __init__(self,
                 mode="upsert",
                 query="",
                 redshift_conn_id="",
                 final_table='',
                 primary_key='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.mode = mode
        self.query = query
        self.redshift_conn_id = redshift_conn_id
        self.final_table = final_table
        self.primary_key = primary_key

    def execute(self, context):    
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        mode = self.mode
        if mode=="append":
            self.log.info('Running {}, Updating with mode: append {}'.format(self.query, self.final_table))
            final_query = LoadFactOperator.append_sql.format(
                query=self.query,
                final_table=self.final_table,
                primary_key=self.primary_key,
            )
        
        else:
            self.log.info('Running {}, Updating with mode: upsert {}'.format(self.query, self.final_table))
            final_query = LoadFactOperator.upsert_sql.format(
                query=self.query,
                final_table=self.final_table,
                primary_key=self.primary_key,
            )
        redshift.run(final_query)
        
