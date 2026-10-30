DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium_user') THEN
        CREATE ROLE debezium_user WITH LOGIN REPLICATION;
    END IF;
END
$$;

ALTER ROLE debezium_user WITH PASSWORD :'debezium_password';

GRANT CONNECT ON DATABASE fire_analytics TO debezium_user;
GRANT USAGE ON SCHEMA public TO debezium_user;
GRANT SELECT ON TABLE public.raw_firms_detections TO debezium_user;

CREATE PUBLICATION firms_detections_publication
    FOR TABLE public.raw_firms_detections
    WITH (publish = 'insert, update, delete');


