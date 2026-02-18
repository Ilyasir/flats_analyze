COPY (
    WITH raw_transformed AS (
        SELECT
            id::BIGINT as id,
            SPLIT_PART(link, '?', 1)::TEXT as link,
            title::VARCHAR as title,
            CASE WHEN title ILIKE '%апартаменты%' THEN TRUE ELSE FALSE END as is_apartament,
            CASE WHEN title ILIKE '%студия%' THEN TRUE ELSE FALSE END as is_studio,
            replace(
                regexp_replace(
                    NULLIF(regexp_extract(title, '([\d\s]+[.,]?\d*)\s*м²', 1), ''),
                    '\s+', '', 'g'
                ),
                ',', '.'
            )::NUMERIC(10, 2) AS area,
            CASE 
                WHEN title ILIKE '%студия%' THEN 0
                ELSE NULLIF(regexp_extract(title, '^(\d+)', 1), '')::INT
            END as rooms_count,
            NULLIF(regexp_extract(title, '(\d+)/\d+\s*этаж', 1), '')::INT as floor,
            NULLIF(regexp_extract(title, '\d+/(\d+)\s*этаж', 1), '')::INT as total_floors,
            regexp_replace(price, '[^0-9]', '', 'g')::BIGINT as price,
            address::TEXT as address,
            SPLIT_PART(address, ',', 1)::VARCHAR as city,
            NULLIF(regexp_extract(address, '([А-Яа-я]+АО)', 1), '')::VARCHAR as okrug,
            CASE
                WHEN okrug IN ('НАО', 'ТАО') THEN NULL
                ELSE 
                    regexp_replace(
                        NULLIF(regexp_extract(address, '(р-н\s?[^,]+)', 1), ''), 
                        '^р-н\s*', '', 'i'
                    )::VARCHAR
            END as district,
            CASE WHEN okrug IN ('НАО', 'ТАО') THEN TRUE ELSE FALSE END as is_new_moscow,
            NULLIF(regexp_extract(metro, '^(.*?)\d+\s+минут', 1), '')::VARCHAR as metro_name,
            NULLIF(regexp_extract(metro, '(\d+)\s+минут', 1), '')::INT as metro_min,
            CASE
                WHEN metro LIKE '%пешком%' THEN 'walk'
                WHEN metro LIKE '%транс%' THEN 'transport'
            END as metro_type,
            parsed_at::TIMESTAMP as parsed_at,
            description::TEXT as description,
            lower(regexp_replace(address, '[^а-яА-Я0-9]', '', 'g')) as norm_address,
            md5(concat_ws('|', norm_address, floor, total_floors, rooms_count)) as flat_hash
        FROM read_json_auto('{{ raw_s3_key }}')
    ),
    deduplicated AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY norm_address, floor, total_floors, rooms_count
                ORDER BY parsed_at DESC, id DESC
            ) as row_num
        FROM raw_transformed
        WHERE area IS NOT NULL 
            AND price IS NOT NULL
            AND okrug IS NOT NULL
            AND rooms_count IS NOT NULL
            AND (district IS NOT NULL OR is_new_moscow)
            AND price / NULLIF(area, 0) > 50000
    )
    SELECT * EXCLUDE (row_num, norm_address)
    FROM deduplicated
    WHERE row_num = 1
) TO '{{ silver_s3_key }}' (FORMAT PARQUET, OVERWRITE TRUE);