CREATE MATERIALIZED VIEW customer_ltv
    IN CLUSTER compute
    AS
    SELECT user_id, name
    FROM app.ingest.users;
