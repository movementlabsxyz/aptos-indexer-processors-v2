-- Update current_aptos_names view: change .apt to .move and update collection IDs to MNS
CREATE OR REPLACE VIEW current_aptos_names AS 
SELECT 
    cal.domain,
    cal.subdomain,
    cal.token_name,
    cal.token_standard,
    cal.registered_address,
    cal.expiration_timestamp,
    greatest(cal.last_transaction_version,
    capn.last_transaction_version) as last_transaction_version,
    coalesce(not capn.is_deleted,
    false) as is_primary,
    concat(cal.domain, '.move') as domain_with_suffix,
    c.owner_address as owner_address,
    cal.expiration_timestamp >= CURRENT_TIMESTAMP as is_active
FROM current_ans_lookup_v2 cal
LEFT JOIN current_ans_primary_name_v2 capn
ON 
    cal.token_name = capn.token_name 
    AND cal.token_standard = capn.token_standard
JOIN current_token_datas_v2 b
ON
    cal.token_name = b.token_name
    AND cal.token_standard = b.token_standard
JOIN current_token_ownerships_v2 c
ON
    b.token_data_id = c.token_data_id
    AND b.token_standard = c.token_standard
WHERE
    cal.is_deleted IS false
    AND c.amount > 0
    AND b.collection_id IN (
        '0x139729cef2e42600f7f1a3d448976831d1c6fc6fc942c9160af4799dd2f581d7' -- Testnet Bardock MNS
    );
