CREATE DATABASE IF NOT EXISTS retail_test;

USE retail_test;

CREATE TABLE region (
  r_regionkey INTEGER PRIMARY KEY NOT NULL,
  r_name      TEXT NOT NULL,
  r_comment   TEXT
);
