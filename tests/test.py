import os, sys
import pretty_errors

current_dir = os.path.dirname(os.path.abspath(__file__))  # tests/
project_root = os.path.abspath(os.path.join(current_dir, '..'))  # root of project

sys.path.insert(0, project_root)


from etl.spark_config import get_spark_config, get_database_config


spark = get_spark_config()

# Choose the DB
db_config = get_database_config('datawarehouse_test')  

df = spark.read \
    .format("jdbc") \
    .option("url", db_config['url']) \
    .option("driver", db_config['driver']) \
    .option("dbtable", "dim_date") \
    .load()



df.show()