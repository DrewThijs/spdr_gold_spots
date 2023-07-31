DROP TABLE IF EXISTS spdr_schema.spdr_gold_spots;


CREATE TABLE IF NOT EXISTS spdr_schema.spdr_gold_spots (
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