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

-- Create the user and grant privileges
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_roles
      WHERE rolname = 'sparkuser'
   ) THEN
      EXECUTE 'CREATE USER sparkuser WITH PASSWORD ''passer''';
   END IF;
   
   EXECUTE 'GRANT ALL PRIVILEGES ON DATABASE sparkdb TO sparkuser';
END
$do$;
