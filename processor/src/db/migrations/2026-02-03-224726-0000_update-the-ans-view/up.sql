-- Your SQL goes here
DROP VIEW IF EXISTS current_aptos_names;
CREATE VIEW current_aptos_names AS 
SELECT cal.domain,
    cal.subdomain,
    cal.token_name,
    cal.token_standard,
    cal.registered_address,
    cal.expiration_timestamp,
    greatest(
    cal.last_transaction_version,
    capn.last_transaction_version
    ) as last_transaction_version,
    coalesce(not capn.is_deleted, false) as is_primary,
    concat(cal.domain, '.move') as domain_with_suffix,
    c.owner_address as owner_address,
    -- is_active is the only change in this migration
    -- subdomain expiration policy of 1 means the name follows the domain expiration
    CASE
        WHEN cal.subdomain_expiration_policy = 1 THEN cal2.expiration_timestamp >= current_timestamp
        ELSE cal.expiration_timestamp >= CURRENT_TIMESTAMP
    END AS is_active,
    cal2.expiration_timestamp as domain_expiration_timestamp,
    b.token_data_id as token_data_id,
    cal.subdomain_expiration_policy as subdomain_expiration_policy
FROM current_ans_lookup_v2 cal
    LEFT JOIN current_ans_primary_name_v2 capn ON cal.token_name = capn.token_name
    AND cal.token_standard = capn.token_standard
    AND capn.registered_address = cal.registered_address 
    JOIN current_token_datas_v2 b ON cal.token_name = b.token_name
    AND cal.token_standard = b.token_standard
    JOIN current_token_ownerships_v2 c ON b.token_data_id = c.token_data_id
    AND b.token_standard = c.token_standard
    LEFT JOIN current_ans_lookup_v2 cal2 ON cal.domain = cal2.domain
    AND cal2.subdomain = ''
    AND cal.token_standard = cal2.token_standard
WHERE cal.is_deleted IS false
    AND c.amount > 0
    AND b.collection_id IN (
        '0x139729cef2e42600f7f1a3d448976831d1c6fc6fc942c9160af4799dd2f581d7' -- Testnet Bardock MNS
    );