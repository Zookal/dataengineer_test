\c retail_etl_dw;


/***********************************************/
/*************  TABLE DEFINITIONS  *************/
/***********************************************/
CREATE TABLE IF NOT EXISTS dim_part (
  p_partkey     INTEGER PRIMARY KEY NOT NULL,
  p_id          INTEGER NOT NULL,
  p_name        VARCHAR(255) NOT NULL,
  p_mfgr        VARCHAR(255) NOT NULL,
  p_brand       VARCHAR(255) NOT NULL,
  p_type        VARCHAR(255) NOT NULL,
  p_size        INTEGER NOT NULL,
  p_container   VARCHAR(255) NOT NULL,
  created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT dim_part_secondary_key UNIQUE (p_id)
);

CREATE TABLE IF NOT EXISTS dim_supplier (
  s_suppkey   INTEGER PRIMARY KEY NOT NULL,
  s_id        INTEGER NOT NULL,
  s_name      VARCHAR(255) NOT NULL,
  s_address   TEXT NOT NULL,
  s_nation    TEXT NOT NULL,
  s_region    TEXT NOT NULL,
  s_phone     VARCHAR(255) NOT NULL,
  created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT  dim_supplier_secondary_key UNIQUE (s_id)
);

CREATE TABLE IF NOT EXISTS dim_customer (
  c_custkey     INTEGER PRIMARY KEY NOT NULL,
  c_id          INTEGER NOT NULL,
  c_name        VARCHAR(255) NOT NULL,
  c_address     TEXT NOT NULL,
  c_nation      TEXT NOT NULL,
  c_region      TEXT NOT NULL,
  c_phone       VARCHAR(255) NOT NULL,
  c_mktsegment  VARCHAR(255) NOT NULL,
  c_cluster     VARCHAR(255) NOT NULL,
  created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT    dim_customer_secondary_key UNIQUE (c_id)
);

CREATE TABLE IF NOT EXISTS dim_date (
  d_datekey               INTEGER PRIMARY KEY NOT NULL,
  d_id                    INTEGER NOT NULL,
  d_date                  DATE NOT NULL,
  d_dayofweek             VARCHAR(255) NOT NULL,
  d_month                 INTEGER NOT NULL,
  d_year                  INTEGER NOT NULL,
  d_monthname             VARCHAR(255) NOT NULL,
  d_yearweek              INTEGER NOT NULL,
  d_yearmonth             VARCHAR(255) NOT NULL,
  d_quarter               VARCHAR(255) NOT NULL,
  d_yearquarter           VARCHAR(255) NOT NULL,
  created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT    dim_date_seconday_key UNIQUE (d_id)
);

CREATE TABLE IF NOT EXISTS fact_lineitem (
  l_id                    SERIAL PRIMARY KEY NOT NULL,
  l_linenumber            INTEGER NOT NULL,
  l_orderkey              INTEGER NOT NULL,
  l_partkey               INTEGER NOT NULL,
  l_suppkey               INTEGER NOT NULL,
  l_custkey               INTEGER NOT NULL,
  l_orderdatekey          INTEGER NOT NULL,
  l_commitdatekey         INTEGER NOT NULL,
  l_receiptdatekey        INTEGER NOT NULL,
  l_shipmode              VARCHAR(255),
  l_quantity              INTEGER NOT NULL,
  l_extendedprice         DECIMAL(10, 2) NOT NULL,
  l_discount              DECIMAL(5, 2) NOT NULL,
  l_revenue               DECIMAL(10, 2) NOT NULL,
  l_tax                   DECIMAL(5, 2) NOT NULL,
  created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (l_partkey) REFERENCES dim_part(p_partkey),
  FOREIGN KEY (l_custkey) REFERENCES dim_customer(c_custkey),
  FOREIGN KEY (l_orderdatekey) REFERENCES dim_date(d_datekey),
  FOREIGN KEY (l_commitdatekey) REFERENCES dim_date(d_datekey),
  FOREIGN KEY (l_receiptdatekey) REFERENCES dim_date(d_datekey),
  CONSTRAINT fact_lineitem_idx UNIQUE (l_orderkey, l_linenumber)
);


/**************************************************/
/*************  FUNCTION DEFINITIONS  *************/
/**************************************************/
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';


/**************************************************/
/**************  TRIGGER DEFINITIONS  *************/
/**************************************************/
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

CREATE TRIGGER update_dim_supplier_updated_at
    BEFORE UPDATE
    ON dim_supplier
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
