from pyhive import hive
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HiveQueries:
    def __init__(self, host, hdfs_path_proc):
        self.conn = hive.Connection(host=host)
        self.hdfs_path_proc = hdfs_path_proc

    def validate_schema(self, schema):
        """
        Validate schema format. Basic validation for simplicity.
        """
        required_types = ['STRING', 'INT', 'DOUBLE', 'DATE']
        for field in schema.split(','):
            parts = field.strip().split()
            if len(parts) != 2 or parts[1] not in required_types:
                raise ValueError(f"Invalid schema field: {field}")
        logger.info("Schema validated successfully.")

    def create_table(self, table_name, schema, hdfs_path):
        try:
            self.validate_schema(schema)
            query = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
                {schema}
            )
            STORED AS PARQUET
            LOCATION '{hdfs_path}'
            """
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                logger.info(f"Created table {table_name}.")
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")

    def create_table_with_dynamic_schema(self, filename, schema_key):
        schemas = {
            'owid': """
            date_ DATE,
            location STRING,
            new_cases INT,
            new_deaths INT,
            total_cases INT,
            total_deaths INT
            """,
            'google_cloud': """
            date_ DATE,
            location STRING,
            new_cases INT,
            new_deaths INT,
            total_cases INT,
            total_deaths INT
            """,
            'csse_confirmed': """
            country STRING,
            state STRING,
            lat DOUBLE,
            long DOUBLE,
            date_ DATE,
            cases INT
            """,
            'csse_deaths': """
            country STRING,
            state STRING,
            lat DOUBLE,
            long DOUBLE,
            date_ DATE,
            deaths INT
            """,
            'who': """
            date_ DATE,
            country STRING,
            new_cases INT,
            new_deaths INT,
            cumulative_cases INT,
            cumulative_deaths INT
            """
        }
        schema = schemas.get(schema_key, "")
        if schema:
            hdfs_path = f'{self.hdfs_path_proc}/{schema_key}/{filename}_cleaned.parquet'
            self.create_table(f'{schema_key}_data', schema, hdfs_path)
            self.load_data_into_table(f'{schema_key}_data', hdfs_path)
        else:
            logger.error(f"Schema key '{schema_key}' not found.")

    def load_data_into_table(self, table_name, hdfs_path):
        try:
            query = f"LOAD DATA INPATH '{hdfs_path}' INTO TABLE {table_name}"
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                logger.info(f"Loaded data into table {table_name} from {hdfs_path}.")
        except Exception as e:
            logger.error(f"Error loading data into table {table_name}: {e}")

    def run_all(self):
        schema_to_filename = {
            'owid': 'full_data',
            'csse_confirmed': 'time_series_covid19_confirmed_global',
            'google_cloud': 'main',
            'csse_deaths': 'time_series_covid19_deaths_global',
            'who': 'WHO-COVID-19-global-data'
        }
        for schema_key in ['owid', 'google_cloud', 'csse_confirmed', 'csse_deaths', 'who']:
            self.create_table_with_dynamic_schema(schema_to_filename.get(schema_key, "default_filename"), schema_key)
