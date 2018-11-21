create database if not exists db;
use db;

source "/tpcds/jimo/date_dim.sql";
source "/tpcds/jimo/item.sql";
source "/tpcds/jimo/warehouse.sql";
source "/tpcds/jimo/call_center.sql";
source "/tpcds/jimo/catalog_page.sql";
source "/tpcds/jimo/customer_address.sql";
source "/tpcds/jimo/customer_demographics.sql";
source "/tpcds/jimo/customer.sql";
source "/tpcds/jimo/household_demographics.sql";
source "/tpcds/jimo/promotion.sql";
source "/tpcds/jimo/reason.sql";
source "/tpcds/jimo/ship_mode.sql";
source "/tpcds/jimo/store.sql";
source "/tpcds/jimo/web_page.sql";
source "/tpcds/jimo/web_site.sql";
