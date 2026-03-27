-- Create conduktor user and database
CREATE USER conduktor WITH PASSWORD 'conduktor';
CREATE DATABASE conduktor OWNER conduktor;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE conduktor TO conduktor;

\connect conduktor

-- Grant schema privileges
GRANT ALL PRIVILEGES ON SCHEMA public TO conduktor;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO conduktor;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO conduktor;
