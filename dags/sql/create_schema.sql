
CREATE TABLE IF NOT EXISTS {{ params.schema_name }}.{{ params.table_name }} (
    "date"                  TIMESTAMP,
    gld_close               FLOAT,
    lbma_gold_price         FLOAT,
    nav_per_gld             FLOAT,
    nav_per_share           FLOAT,
    indicative_price        FLOAT,
    daily_share_volume      FLOAT,
    total_nav_ounces        FLOAT,
    total_nav_tonnes        FLOAT,
    total_nav_value         FLOAT
);