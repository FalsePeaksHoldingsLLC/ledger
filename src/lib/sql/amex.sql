SELECT
    date,
    LOWER(description) AS description,
    amount,
    'amex' AS account_type
FROM
    amex