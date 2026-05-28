CREATE TABLE addresses (
    account_id    bigint       NOT NULL,
    line1         text         NOT NULL,
    city          text         NOT NULL,
    region        text,
    country       text         NOT NULL
);
