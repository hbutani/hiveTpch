-- specify datadir as -hiveconf tpch.data.dir=<path to data>
-- specify database as -hiveconf tpch.db=<e.g. tpch1>

set hive.variable.substitute=true;

create database if not exists ${hiveconf:tpch.db};

use ${hiveconf:tpch.db};

drop table if exists region;
CREATE TABLE region( 
    r_regionkey INT, 
    r_name STRING, 
    r_comment STRING
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
;

drop table if exists nation;
CREATE TABLE nation( 
    n_nationkey INT, 
    n_name STRING, 
    n_regionkey INT, 
    n_comment STRING
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
;

drop table if exists customer;
CREATE TABLE customer( 
    c_custkey INT, 
    c_name STRING,
    c_address STRING,
    c_nationkey INT, 
    c_phone STRING,
    c_acctbal DOUBLE, 
    c_mktsegment STRING,
    c_comment STRING,
    c_n_name STRING, 
    c_n_regionkey INT, 
    c_n_comment STRING,
    c_r_name STRING, 
    c_r_comment STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED AS RCFILE
;

drop table if exists part;
create table part (
    p_partkey INT,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DOUBLE,
    p_comment STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED AS RCFILE
;

drop table if exists supplier;
CREATE TABLE supplier( 
    s_suppkey INT, 
    s_name STRING,
    s_address STRING,
    s_nationkey INT, 
    s_phone STRING,
    s_acctbal DOUBLE,
    s_comment STRING,
    s_n_name STRING, 
    s_n_regionkey INT, 
    s_n_comment STRING,
    s_r_name STRING, 
    s_r_comment STRING
) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED AS RCFILE
;

drop table if exists partsupp;
CREATE TABLE partsupp( 
    ps_partkey INT, 
    ps_suppkey INT, 
    ps_availqty INT, 
    ps_supplycost DOUBLE, 
    ps_comment STRING,
    s_name STRING,
    s_address STRING,
    s_nationkey INT, 
    s_phone STRING,
    s_acctbal DOUBLE,
    s_comment STRING,
    s_n_name STRING, 
    s_n_regionkey INT, 
    s_n_comment STRING,
    s_r_name STRING, 
    s_r_comment STRING,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DOUBLE,
    p_comment STRING
) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED AS RCFILE
;

drop table if exists orders;
CREATE TABLE orders( 
    o_orderkey INT, 
    o_custkey INT, 
    o_orderstatus STRING,
    o_totalprice DOUBLE, 
    o_orderdate timestamp, 
    o_orderpriority STRING,
    o_clerk STRING,
    o_shippriority INT, 
    o_comment STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED AS RCFILE
;

drop table if exists lineitem;
CREATE TABLE lineitem( 
    l_orderkey INT, 
    l_partkey INT, 
    l_suppkey INT, 
    l_linenumber INT, 
    l_quantity DOUBLE, 
    l_extendedprice DOUBLE, 
    l_discount DOUBLE, 
    l_tax DOUBLE, 
    l_returnflag STRING,
    l_linestatus STRING,
    l_shipdate timestamp, 
    l_commitdate timestamp, 
    l_receiptdate timestamp, 
    l_shipinstruct STRING,
    l_shipmode STRING,
    l_comment STRING
) 
PARTITIONED BY (ship_year int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED AS RCFILE
;
MSCK REPAIR TABLE lineitem;
