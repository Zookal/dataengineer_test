\c retail_etl_dw;

/*************  TABLE DEFINITIONS  *************/
CREATE TABLE IF NOT EXISTS dim_part (
  p_partkey     INTEGER PRIMARY KEY NOT NULL,
  p_name        VARCHAR(255) NOT NULL,
  p_mfgr        VARCHAR(255) NOT NULL,
  p_brand       VARCHAR(255) NOT NULL,
  p_type        VARCHAR(255) NOT NULL,
  p_size        INTEGER NOT NULL,
  p_container   VARCHAR(255) NOT NULL,
  created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_supplier (
  s_suppkey   INTEGER PRIMARY KEY NOT NULL,
  s_name      VARCHAR(255) NOT NULL,
  s_address   TEXT NOT NULL,
  s_nation    TEXT NOT NULL,
  s_phone     VARCHAR(255) NOT NULL,
  created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_shipmode (
  sh_shipmodekey   INTEGER PRIMARY KEY NOT NULL,
  sh_name          VARCHAR(255) NOT NULL,
  created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_customer (
  c_custkey    INTEGER PRIMARY KEY NOT NULL,
  c_name       VARCHAR(255) NOT NULL,
  c_address    TEXT NOT NULL,
  c_nation     TEXT NOT NULL,
  c_region     TEXT NOT NULL,
  c_phone      VARCHAR(255) NOT NULL,
  c_mktsegment VARCHAR(255) NOT NULL,
  created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_date (
  d_datekey               INTEGER PRIMARY KEY NOT NULL,
  d_date                  DATE NOT NULL,
  d_fulldatedescription   VARCHAR(255) NOT NULL,
  d_dayofweek             VARCHAR(255) NOT NULL,
  d_month                 INTEGER NOT NULL,
  d_year                  INTEGER NOT NULL,
  d_monthname             VARCHAR(255) NOT NULL,
  d_monthnameyear         VARCHAR(255) NOT NULL,
  d_weeknuminyear         INTEGER NOT NULL,
  d_daynuminyear          INTEGER NOT NULL,
  d_daynuminmonth         INTEGER NOT NULL,
  d_yearmonth             VARCHAR(255) NOT NULL,
  d_quarter               VARCHAR(255) NOT NULL,
  d_yearquarter           VARCHAR(255) NOT NULL,
  created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_lineitem (
  l_id                    INTEGER PRIMARY KEY NOT NULL,
  l_linenumber            INTEGER NOT NULL,
  l_orderkey              INTEGER NOT NULL,
  l_partkey               INTEGER NOT NULL,
  l_suppkey               INTEGER NOT NULL,
  l_custkey               INTEGER NOT NULL,
  l_orderdatekey          INTEGER NOT NULL,
  l_commitdatekey         INTEGER NOT NULL,
  l_shipmodekey           INTEGER NOT NULL,
  l_ordetotalprice        DECIMAL(10, 2) NOT NULL,
  l_quantity              INTEGER NOT NULL,
  l_extendedprice         DECIMAL(10, 2) NOT NULL,
  l_discount              DECIMAL(5, 2) NOT NULL,
  l_revenue               DECIMAL(10, 2) NOT NULL,
  l_tax                   DECIMAL(5, 2) NOT NULL,
  created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

/*************  FUNCTION DEFINITIONS  *************/
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

/*************  TRIGGER DEFINITIONS  *************/
CREATE TRIGGER update_dim_shipmode_updated_at
    BEFORE UPDATE
    ON dim_shipmode
    FOR EACH ROW
    EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER update_dim_date_updated_at
    BEFORE UPDATE
    ON dim_date
    FOR EACH ROW
    EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER update_dim_part_updated_at
    BEFORE UPDATE
    ON dim_part
    FOR EACH ROW
    EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER update_dim_customer_updated_at
    BEFORE UPDATE
    ON dim_customer
    FOR EACH ROW
    EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER update_fact_lineitem_updated_at
    BEFORE UPDATE
    ON fact_lineitem
    FOR EACH ROW
    EXECUTE PROCEDURE update_updated_at_column();
