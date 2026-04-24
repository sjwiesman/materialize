CREATE MATERIALIZED VIEW customer_ltv
    IN CLUSTER :"compute_cluster"
    AS
    SELECT user_id, name, email
    FROM app.ingest.users;
