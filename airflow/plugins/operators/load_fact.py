from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    create_sql ="""
    DROP TABLE IF EXISTS temp_table;
    
    CREATE TABLE temp_table AS(
    {query}
    );
    """

    delete_sql ="""
    DELETE FROM {final_table}
    WHERE {primary_key} IN (SELECT {primary_key} FROM temp_table);
    """
    
    insert_sql ="""
    INSERT INTO {final_table}
    (SELECT * FROM temp_table);
    
    """
    

    @apply_defaults
    def __init__(self,
                 upsert=True,
                 query="",
                 redshift_conn_id="",
                 final_table='',
                 primary_key='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.upsert = upsert
        self.query = query
        self.redshift_conn_id = redshift_conn_id
        self.final_table = final_table
        self.primary_key = primary_key

    def execute(self, context):    
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        mode = self.mode
        if upsert==True:
            concat_query = LoadFactOperator.create_sql + LoadFactOperator.delete_sql + LoadFactOperator.insert_sql
            final_query = concat_query.format(
                query=self.query,
                final_table=self.final_table,
                primary_key=self.primary_key
            )
        
        else:
            concat_query = LoadFactOperator.create_sql + LoadFactOperator.insert_sql
            final_query = concat_query.format(
                query=self.query,
                final_table=self.final_table,
                primary_key=self.primary_key
            )

        
        self.log.info('Running {}, For table {}'.format(self.query, self.final_table))
        redshift.run(final_query)
        
