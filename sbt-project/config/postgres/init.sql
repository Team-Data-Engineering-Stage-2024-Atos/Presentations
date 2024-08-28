-- Initialize the PostgreSQL database for Spark

-- Create the sparkdb database if it doesn't exist
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_database
      WHERE datname = 'sparkdb'
   ) THEN
      PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE sparkdb');
   END IF;
END
$do$;

-- Create a schema named "spark_catalog" if it doesn't exist
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT 1 FROM information_schema.schemata WHERE schema_name = 'spark_catalog'
   ) THEN
      CREATE SCHEMA spark_catalog;
   END IF;
END
$do$;

-- Create the sparkuser with password and grant privileges
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_roles
      WHERE rolname = 'sparkuser'
   ) THEN
      CREATE USER sparkuser WITH PASSWORD 'passer';
   END IF;

   GRANT ALL PRIVILEGES ON DATABASE sparkdb TO sparkuser;
   GRANT USAGE ON SCHEMA spark_catalog TO sparkuser;
   GRANT CREATE ON SCHEMA spark_catalog TO sparkuser;
   GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA spark_catalog TO sparkuser;
   ALTER DEFAULT PRIVILEGES IN SCHEMA spark_catalog GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO sparkuser;
END
$do$;