# %%
from google.cloud import bigquery
import os 
import textwrap
import pandas_gbq as pdgbq
from google.cloud.bigquery import TimePartitioning
import pandas as pd 



# %%
def create_query_string(sql_file):
    with open(sql_file, 'r') as f_in:
        lines = f_in.read()

        # remove common leading whitespace from all lines    
        query_string = textwrap.dedent("""{}""".format(lines))

        return query_string

# %%
class BqUtils():
    def __init__(self, project_id, credentials = None):
        self.project_id = project_id 
        self.credentials = credentials
        
        if credentials is not None:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials 

        import google.auth
        SCOPE = ('https://www.googleapis.com/auth/bigquery',
                'https://www.googleapis.com/auth/cloud-platform',
                'https://www.googleapis.com/auth/drive')
        credentials, project_id = google.auth.default(scopes=SCOPE)
        pdgbq.context.credentials = credentials
        
        self.bq_client = bigquery.Client(project_id, credentials)
    
    def get_table(self, dataset_id, table_name):
        dataset = self.bq_client.dataset(dataset_id = dataset_id, project = self.project_id)
        table_ref = dataset.table(table_name) 
        table = self.bq_client.get_table(table_ref)
        
        return table
    
    def get_match_date_range(self, dataset_id, table_name):
        sql = f"""
            select
              min(match_date) as min_match_date,
              max(match_date) as max_match_date,
            FROM `alt-tab-348721.{dataset_id}.{table_name}` 
        """
        
        query_job = self.bq_client.query(sql)
        res = query_job.result() 
        
        rows = []
        for r in res:
           row = {} 
           for k, v in r.items():
              row[k] = v 
           rows.append(row)

        return rows 
    
    def get_table_information(self, dataset_id, table_name):
        table = self.get_table(dataset_id = dataset_id, table_name = table_name)
        match_date_range = self.get_match_date_range(dataset_id, table_name)
        
        info= {}
        info["num_rows"] = table.num_rows
        info["min_match_date"] = match_date_range[0]["min_match_date"]
        info["max_match_date"] = match_date_range[0]["max_match_date"]
        
        return info 
    
    def run_query(self, sql, dataset_id, table_name = None, match_date = None):
        
        
        
        if match_date is not None:
            query = create_query_string(sql) \
                        .format(
                           match_date = match_date,
                           dataset_id = dataset_id
                        )
                        
            res = self.bq_client.query(query).result()
            
            rows = []
            
            for r in res:
               row = {} 
               for k, v in r.items(): 
                  row[k] = v 
               rows.append(row)

            this_week_result = pd.DataFrame(rows)
            this_week_result["total_point_delta"] = this_week_result["game_difference_point"]  \
                                                   + this_week_result["winning_point"]
            display(this_week_result.sort_values("total_point_delta", ascending = False))
            
        else:
            query = create_query_string(sql) \
                        .format(
                           dataset_id = dataset_id,
                           table_name = table_name
                        )

            before_table_info = self.get_table_information(dataset_id, table_name)
            before_min_date = before_table_info["min_match_date"]
            before_max_date = before_table_info["max_match_date"]
            before_num_rows = before_table_info["num_rows"]
        
            res = self.bq_client.query(query).result()
        
            after_table_info = self.get_table_information(dataset_id, table_name)
            after_min_date = after_table_info["min_match_date"]
            after_max_date = after_table_info["max_match_date"]
            after_num_rows = after_table_info["num_rows"]
            
            print(f"===== SEASON: {dataset_id} / TABLE: {table_name} =====")
            print("- before table num rows: ", before_num_rows)
            print(f"- before match date ragne: {before_min_date} ~ {before_max_date}")

            print("- after table num rows: ", after_num_rows)
            print(f"- after match date ragne: {after_min_date} ~ {after_max_date}")

            print(f"Successfuly updated table.")
    
        
    def overwrite_table(self, sql, dataset_id, table_name, match_date = None, player_name = None, partition = True):
        if partition:
            create_query = create_query_string("sql/000_create_or_replace.bqsql")
        else: 
            create_query = create_query_string("sql/000_create_or_replace_wo_partition.bqsql")
        
        sql_query = create_query_string(sql)
        query = create_query + sql_query
        
        format_list = {"dataset_id": dataset_id, 
                       "table_name": table_name, 
                       "match_date": match_date,
                       "player_name": player_name}
        
        format_input = {}
        for k, v in format_list.items():
            if v is not None: format_input[k] = v
        
        query_formated = query.format(**format_input)
        
        before_table_info = self.get_table_information(dataset_id, table_name)
        before_min_date = before_table_info["min_match_date"]
        before_max_date = before_table_info["max_match_date"]
        before_num_rows = before_table_info["num_rows"]
        
        res = self.bq_client.query(query_formated).result()
        
        after_table_info = self.get_table_information(dataset_id, table_name)
        after_min_date = after_table_info["min_match_date"]
        after_max_date = after_table_info["max_match_date"]
        after_num_rows = after_table_info["num_rows"]
        
        print(f"===== SEASON: {dataset_id} / TABLE: {table_name} =====")
        print("- before table num rows: ", before_num_rows)
        print(f"- before match date ragne: {before_min_date} ~ {before_max_date}")
        
        print("- after table num rows: ", after_num_rows)
        print(f"- after match date ragne: {after_min_date} ~ {after_max_date}")
        
        print(f"Successfuly overwrited table.")


        

# %%
