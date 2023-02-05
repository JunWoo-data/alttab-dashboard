# %%
from utils.bqutils import BqUtils, create_query_string
import pandas as pd 


# %%
PROJECT_ID = "alt-tab-348721"
CREDENTIALS = "./utils/alttab_dashboard_credentials.json"

# %%
bqutils = BqUtils(PROJECT_ID, CREDENTIALS)

# %%
dataset_id = "alttab_23w"
dataset_id_all = "alttab_all"

# %%
#######################################
#######################################
###### match_result_faltten table #####
#######################################
#######################################

bqutils.overwrite_table(
   sql = "sql/101_match_result_flatten.bqsql", 
   dataset_id = dataset_id,
   table_name = "match_result_flatten",
   partition = True
)

# %%
##########################################
##########################################
###### check weekly rank point delta #####
##########################################
##########################################

match_date = "'2023-02-02'" # TODO: change the match_date

res = bqutils.run_query(
   sql = "sql/101_check_weekly_result.bqsql", 
   dataset_id = dataset_id, 
   match_date = match_date
)

# %%
##########################################
##########################################
###### match_result_duplicated table #####
##########################################
##########################################

bqutils.overwrite_table(
   sql = "sql/101_match_result_duplicated.bqsql", 
   dataset_id = dataset_id,
   table_name = "match_result_duplicated",
   partition = True
)

# %%
###############################
###############################
###### update_player_rank #####
###############################
###############################

bqutils.run_query(
   sql = "sql/102_update_player_rank.bqsql",
   dataset_id = dataset_id, 
   table_name = "player_rank"
)

# %%
################################################
################################################
###### update_absence_info_for_player_rank #####
################################################
################################################

absence_player_list = ["정선민", "곽대희", "정광우"] # TODO: change the player names
match_date = "2023-02-02" # TODO: change the match date

for player_name in absence_player_list:
   bqutils.overwrite_table(
      sql = "sql/102_update_absence_info_for_player_rank.bqsql", 
      dataset_id = dataset_id, 
      table_name = "player_rank", 
      partition = True,
      match_date = match_date,
      player_name = player_name
   )   



# %%
###############################
###############################
###### standing_table ########
###############################
###############################

bqutils.overwrite_table(
   sql = "sql/104_standing_table.bqsql",
   dataset_id = dataset_id, 
   table_name = "standing_table",
   partition = True
)

# %%
###########################################
###########################################
###### match_result_duplicated_all ########
###########################################
###########################################

bqutils.overwrite_table(
   sql = "sql/201_match_result_duplicated_all.bqsql",
   dataset_id = dataset_id_all, 
   table_name = "match_result_duplicated_all",
   partition = True
)

# %%
##################################
##################################
###### standing_table_all ########
##################################
##################################

bqutils.overwrite_table(
   sql = "sql/202_standing_table_all.bqsql",
   dataset_id = dataset_id_all, 
   table_name = "standing_table_all",
   partition = True
)

# %%
##################################
##################################
###### stat_partner ##############
##################################
##################################

bqutils.overwrite_table(
   sql = "sql/301_stat_partner.bqsql",
   dataset_id = dataset_id_all, 
   table_name = "stat_partner",
   partition = True
)

# %%
##################################
##################################
###### stat_oponent ##############
##################################
##################################

bqutils.overwrite_table(
   sql = "sql/302_stat_opponent.bqsql",
   dataset_id = dataset_id_all, 
   table_name = "stat_opponent",
   partition = True
)