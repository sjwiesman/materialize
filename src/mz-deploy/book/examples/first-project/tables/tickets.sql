CREATE TABLE tickets (
    id               bigint        NOT NULL,
    opened_at        timestamptz   NOT NULL,
    sla_minutes      integer       NOT NULL,
    closed_at        timestamptz
);
