-- specify datadir as -hiveconf tpch.data.dir=<path to data>
-- specify database as -hiveconf tpch.db=<e.g. tpch1>

set hive.variable.substitute=true;
use ${hiveconf:tpch.db};

SET hive.exec.compress.output=true;
SET mapred.max.split.size=256000000;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

load data inpath '${hiveconf:tpch.data.dir}/region.tbl' 
overwrite into table region;

load data inpath '${hiveconf:tpch.data.dir}/nation.tbl' 
overwrite into table nation;

drop table if exists e_customer;
CREATE external TABLE e_customer( 
    c_custkey INT, 
    c_name STRING,
    c_address STRING,
    c_nationkey INT, 
    c_phone STRING,
    c_acctbal DOUBLE, 
    c_mktsegment STRING,
    c_comment STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
STORED AS TEXTFILE
location '${hiveconf:tpch.data.dir}/customer/' 
;

insert overwrite table customer
select /*+ mapjoin(nr) */
    c_custkey, 
    c_name,
    c_address,
    c_nationkey, 
    c_phone,
    c_acctbal, 
    c_mktsegment,
    c_comment,
    n_name, 
    n_regionkey, 
    n_comment,
    r_name, 
    r_comment
from e_customer c join (
                   select *
                   from nation n join region r on n.n_regionkey = r.r_regionkey
                   ) nr on c.c_nationkey = nr.n_nationkey
;
analyze table customer compute statistics;
drop table e_customer;

drop table if exists e_part;
create external table e_part (
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
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
location '${hiveconf:tpch.data.dir}/part/'
;

insert overwrite table part
select * from e_part;
analyze table part compute statistics;
drop table e_part;

drop table if exists e_supplier;
CREATE external TABLE e_supplier( 
    s_suppkey INT, 
    s_name STRING,
    s_address STRING,
    s_nationkey INT, 
    s_phone STRING,
    s_acctbal DOUBLE,
    s_comment STRING
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
location '${hiveconf:tpch.data.dir}/supplier/'
;

insert overwrite table supplier
select /*+ mapjoin(nr) */
    s_suppkey, 
    s_name,
    s_address,
    s_nationkey, 
    s_phone,
    s_acctbal,
    s_comment,
    n_name, 
    n_regionkey, 
    n_comment,
    r_name, 
    r_comment
from e_supplier s join (
                   select *
                   from nation n join region r on n.n_regionkey = r.r_regionkey
                   ) nr on s.s_nationkey = nr.n_nationkey
;
analyze table supplier compute statistics;
drop table e_supplier;

drop table if exists e_partsupp;
CREATE external TABLE e_partsupp( 
    ps_partkey INT, 
    ps_suppkey INT, 
    ps_availqty INT, 
    ps_supplycost DOUBLE, 
    ps_comment STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
location '${hiveconf:tpch.data.dir}/partsupp/'
;

insert overwrite table partsupp
select 
    ps_partkey, 
    ps_suppkey, 
    ps_availqty, 
    ps_supplycost, 
    ps_comment,
    s_name,
    s_address,
    s_nationkey, 
    s_phone,
    s_acctbal,
    s_comment,
    s_n_name, 
    s_n_regionkey, 
    s_n_comment,
    s_r_name, 
    s_r_comment,
    p_name,
    p_mfgr,
    p_brand,
    p_type,
    p_size,
    p_container,
    p_retailprice,
    p_comment
from e_partsupp ps join part p on ps.ps_partkey = p.p_partkey
                join supplier s on ps.ps_suppkey = s.s_suppkey
;
analyze table partsupp compute statistics;
drop table e_partsupp;

drop table if exists e_orders;
CREATE external TABLE e_orders( 
    o_orderkey INT, 
    o_custkey INT, 
    o_orderstatus STRING,
    o_totalprice DOUBLE, 
    o_orderdate STRING, 
    o_orderpriority STRING,
    o_clerk STRING,
    o_shippriority INT, 
    o_comment STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
location '${hiveconf:tpch.data.dir}/orders/'
;

insert overwrite table orders
select 
    o_orderkey, 
    o_custkey, 
    o_orderstatus,
    o_totalprice, 
    cast(unix_timestamp(o_orderdate,'yyyy-MM-dd')*1000 as timestamp), 
    o_orderpriority,
    o_clerk,
    o_shippriority, 
    o_comment
from e_orders;
analyze table orders compute statistics;
drop table e_orders;

drop table if exists e_lineitem;
CREATE external TABLE e_lineitem( 
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
    l_shipdate STRING, 
    l_commitdate STRING, 
    l_receiptdate STRING, 
    l_shipinstruct STRING,
    l_shipmode STRING,
    l_comment STRING
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
location '${hiveconf:tpch.data.dir}/lineitem/'
;


set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table lineitem
partition(ship_year)
select 
    l_orderkey, 
    l_partkey, 
    l_suppkey, 
    l_linenumber, 
    l_quantity, 
    l_extendedprice, 
    l_discount, 
    l_tax, 
    l_returnflag,
    l_linestatus,
    cast(unix_timestamp(l_shipdate,'yyyy-MM-dd')*1000 as timestamp), 
    cast(unix_timestamp(l_commitdate,'yyyy-MM-dd')*1000 as timestamp), 
    cast(unix_timestamp(l_receiptdate,'yyyy-MM-dd')*1000 as timestamp), 
    l_shipinstruct,
    l_shipmode,
    l_comment,
    year(l_shipdate) as ship_year
from e_lineitem;

drop table e_lineitem;

analyze table lineitem partition(ship_year) compute statistics;
