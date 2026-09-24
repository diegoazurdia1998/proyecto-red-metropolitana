{% macro normalizar_zona(campo_zona) %}
    CASE
        WHEN {{ campo_zona }} IS NULL THEN 'Zona No Definida'
        WHEN UPPER(TRIM({{ campo_zona }})) IN ('Z1', 'ZONA 1', 'ZONE 1') THEN 'Zona 1'
        WHEN UPPER(TRIM({{ campo_zona }})) IN ('Z4', 'ZONA 4', 'ZONE 4') THEN 'Zona 4'
        WHEN UPPER(TRIM({{ campo_zona }})) IN ('Z6', 'ZONA 6', 'ZONE 6') THEN 'Zona 6'
        WHEN UPPER(TRIM({{ campo_zona }})) IN ('Z7', 'ZONA 7', 'ZONE 7') THEN 'Zona 7'
        WHEN UPPER(TRIM({{ campo_zona }})) IN ('Z8', 'ZONA 8', 'ZONE 8') THEN 'Zona 8'
        WHEN UPPER(TRIM({{ campo_zona }})) IN ('Z9', 'ZONA 9', 'ZONE 9') THEN 'Zona 9'
        WHEN UPPER(TRIM({{ campo_zona }})) IN ('Z10', 'ZONA 10', 'ZONE 10') THEN 'Zona 10'
        WHEN UPPER(TRIM({{ campo_zona }})) IN ('Z11', 'ZONA 11', 'ZONE 11') THEN 'Zona 11'
        WHEN UPPER(TRIM({{ campo_zona }})) IN ('Z12', 'ZONA 12', 'ZONE 12') THEN 'Zona 12'
        WHEN UPPER(TRIM({{ campo_zona }})) IN ('Z17', 'ZONA 17', 'ZONE 17') THEN 'Zona 17'
        ELSE INITCAP(TRIM({{ campo_zona }}))
    END
{% endmacro %}