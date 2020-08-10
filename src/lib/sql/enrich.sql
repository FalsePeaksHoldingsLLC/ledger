SELECT
    a.date,
    a.description,
    t.type,
    a.amount,
    a.account_type,
    CASE
        WHEN 
            a.amount > 0
        THEN 
            'debit'
        WHEN
            a.amount < 0
        THEN
            'credit'
        ELSE
            ''
    END AS account_action
FROM
    amex_view a
LEFT JOIN
    tags_view t
ON  
    a.description LIKE concat('%',replace(t.lookup,' ','%'),'%')