SELECT 
    *
FROM {{ source('raw', 'cash') }}