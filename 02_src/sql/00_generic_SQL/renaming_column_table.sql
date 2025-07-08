ALTER TABLE `covid19-1-463112.raw_transverse_dataset.raw_country_dim`
RENAME COLUMN string_field_0 TO country_name,
RENAME COLUMN string_field_1 TO alpha_2_code,
RENAME COLUMN string_field_2 TO alpha_3_code,
RENAME COLUMN string_field_3 TO region,
RENAME COLUMN string_field_4 TO continent
RENAME COLUMN string_field_5 TO area_km2;

---
DELETE FROM `covid19-1-463112.raw_transverse_dataset.country_codes`
WHERE country_name = "country_name";