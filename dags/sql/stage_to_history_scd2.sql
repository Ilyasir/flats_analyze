UPDATE gold.history_flats as h
SET
    effective_to = s.parsed_at,
    is_active = FALSE
FROM gold.stage_flats as s
WHERE h.flat_id = s.flat_id
    AND h.is_active = TRUE
    AND h.price != s.price;

--
INSERT INTO gold.history_flats (
    flat_id, link, title, price, is_apartament, is_studio, area, 
    rooms_count, floor, total_floors, is_new_moscow, address, 
    city, okrug, district, metro_name, metro_min, metro_type, 
    parsed_at, effective_from, is_active
)
SELECT 
    s.flat_id, s.link, s.title, s.price, s.is_apartament, s.is_studio, s.area, 
    s.rooms_count, s.floor, s.total_floors, s.is_new_moscow, s.address, 
    s.city, s.okrug, s.district, s.metro_name, s.metro_min, s.metro_type, 
    s.parsed_at, 
    s.parsed_at as effective_from, 
    TRUE as is_active
FROM gold.stage_flats as s
LEFT JOIN gold.history_flats h
    ON s.flat_id = h.flat_id AND h.is_active = TRUE
WHERE h.flat_id IS NULL
   OR h.price != s.price;

SELECT 
    (SELECT count(*) FROM gold.stage_flats) as total_in_stage,
    (SELECT count(*) FROM gold.history_flats WHERE is_active = TRUE) as active_records_after_merge;