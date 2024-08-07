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
        required_types = ['STRING', 'INT', 'DOUBLE']
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
            """  # Removed the trailing semicolon
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                logger.info(f"Created table {table_name}.")
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")

    def create_table_with_dynamic_schema(self, table_name, schema_key):
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
            hdfs_path = f'{self.hdfs_path_proc}/{schema_key}_cleaned_data'
            self.create_table(f'{schema_key}_data', schema, hdfs_path)
        else:
            logger.error(f"Schema key '{schema_key}' not found.")

    def run_all(self):
        for schema_key in ['owid']:
            self.create_table_with_dynamic_schema(schema_key, schema_key)

if __name__ == "__main__":
    hive_queries = HiveQueries(host='hive-server', hdfs_path_proc='/user/admin/data/processed')
    hive_queries.run_all()

#['owid', 'google_cloud', 'csse_confirmed', 'csse_deaths', 'who']