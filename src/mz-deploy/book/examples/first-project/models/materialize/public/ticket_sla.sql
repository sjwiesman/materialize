CREATE MATERIALIZED VIEW ticket_sla
IN CLUSTER app
AS
SELECT
    id,
    opened_at,
    sla_minutes,
    closed_at,
    CASE
        WHEN closed_at IS NULL
             AND mz_now() > opened_at + (sla_minutes * INTERVAL '1 minute')
        THEN 'breached'
        WHEN closed_at IS NOT NULL
             AND closed_at > opened_at + (sla_minutes * INTERVAL '1 minute')
        THEN 'closed_breached'
        ELSE 'on_time'
    END AS status
FROM raw.tickets;
