CREATE MATERIALIZED VIEW customer
IN CLUSTER app
AS
SELECT
    a.id              AS account_id,
    a.signed_up_at,
    a.status,
    addr.line1        AS address_line1,
    addr.city         AS address_city,
    addr.region       AS address_region,
    addr.country      AS address_country,
    email.value       AS primary_email,
    phone.value       AS phone_number
FROM raw.accounts a
LEFT JOIN raw.addresses addr
       ON addr.account_id = a.id
LEFT JOIN raw.contact_methods email
       ON email.account_id = a.id AND email.kind = 'email'
LEFT JOIN raw.contact_methods phone
       ON phone.account_id = a.id AND phone.kind = 'phone';
