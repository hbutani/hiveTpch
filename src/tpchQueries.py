'''
Created on May 10, 2013

'''

QUERIES = {}

class Query(object):
    def __init__(self, name, sql, preSql=None, postSql=None, comment=None):
        self.name = name
        self.sql = sql
        self.preSql = preSql
        self.postSql = postSql
        self.comment = None
        QUERIES[name] = self
        
Query('q1',
'''select
l_returnflag,
l_linestatus,
sum(l_quantity) as sum_qty,
sum(l_extendedprice) as sum_base_price,
sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
avg(l_quantity) as avg_qty,
avg(l_extendedprice) as avg_price,
avg(l_discount) as avg_disc,
count(*) as count_order
from
lineitem
where
l_shipdate <= cast(unix_timestamp('1998-09-01','yyyy-MM-dd')*1000 as timestamp)
group by
l_returnflag,
l_linestatus
order by
l_returnflag,
l_linestatus;'''
)
Query('q2',
'''select
  s_acctbal,
  s_name,
  s_n_name,
  ps.ps_partkey,
  p_mfgr,
  s_address,
  s_phone,
  s_comment
from
partsupp ps join (
         select ps_partkey, min(ps_supplycost) min_cost
         from partsupp
         where s_r_name = 'EUROPE' 
         group by ps_partkey
  ) mcost on ps.ps_partkey = mcost.ps_partkey and ps.ps_supplycost = mcost.min_cost
where 
  p_size = 15
  and p_type like '%BRASS'
  and s_r_name = 'EUROPE'
order by
s_acctbal desc,
s_n_name,
s_name,
ps.ps_partkey
limit 100;
''')

Query('q2.w' ,
'''select * from (
select
  s_acctbal,
  s_name,
  s_n_name,
  ps_partkey,
  p_mfgr,
  s_address,
  s_phone,
  s_comment,
  rank() over(partition by ps_partkey order by ps_supplycost, s_acctbal) as r
from partsupp
where 
  p_size = 15
  and p_type like '%BRASS'
  and s_r_name = 'EUROPE'
) a
where r = 1
order by
s_acctbal desc,
s_n_name,
s_name,
ps_partkey
limit 100;
''')

Query('q3',
'''select
  l_orderkey,
  sum(l_extendedprice*(1-l_discount)) as revenue,
  o_orderdate,
  o_shippriority
from
 (select * from customer where c_mktsegment = 'BUILDING') c 
 join 
 (select *
  from orders 
  where o_orderdate < cast(unix_timestamp('1995-03-15','yyyy-MM-dd')*1000 as timestamp) ) o 
     on  c.c_custkey = o.o_custkey 
 join 
 (select * 
  from lineitem 
  where l_shipdate > cast(unix_timestamp('1995-03-15','yyyy-MM-dd')*1000 as timestamp) ) l 
     on l.l_orderkey = o.o_orderkey 
group by
l_orderkey,
o_orderdate,
o_shippriority
order by
revenue desc,
o_orderdate
limit 10;
''')

Query('q4',
'''select
  o_orderpriority,
  count(*) as order_count
from orders o left semi join lineitem l on o.o_orderkey = l.l_orderkey and 
                                      l.l_commitdate < l.l_receiptdate and
                                      o.o_orderdate >= cast(unix_timestamp('1993-07-01','yyyy-MM-dd')*1000 as timestamp) and
                                      o.o_orderdate < cast(unix_timestamp('1993-10-01','yyyy-MM-dd') *1000 as timestamp)
group by
o_orderpriority
order by
o_orderpriority;
''')

Query('q5',
'''select
  s_n_name,
  sum(l_extendedprice * (1 - l_discount)) as revenue
from
  customer c join orders o on c.c_custkey = o.o_custkey and
                              year((o.o_orderdate)) = 1994
             join lineitem l on o.o_orderkey = l.l_orderkey
             join supplier s on l.l_suppkey = s.s_suppkey and 
                                c.c_nationkey = s.s_nationkey and s.s_r_name = 'ASIA'
group by
  s_n_name
order by
  revenue desc;
''')

Query('q6',
'''select
  sum(l_extendedprice*l_discount) as revenue
from
  lineitem
where
  ship_year = 1994
  and l_discount between 0.05 and 0.07
  and l_quantity < 24;
''')

Query('q7',
'''select
  supp_nation,
  cust_nation,
  l_year,
  sum(volume) as revenue
from (
select
  s_n_name as supp_nation,
  c_n_name as cust_nation,
  ship_year as l_year,
  l_extendedprice * (1 - l_discount) as volume
from
   (select /*+ mapjoin(s) */ s_n_name, ship_year, l_extendedprice, l_discount, l_orderkey
    from
       supplier s join lineitem l on l.ship_year in (1995, 1996) 
                     and s.s_suppkey = l.l_suppkey
                     and s.s_n_name in ('FRANCE', 'GERMANY')
   ) x join
   (select /*+ mapjoin(c) */ c_n_name, o_orderkey
    from
        orders o join customer c on o.o_custkey = c.c_custkey
                    and c.c_n_name in ('FRANCE', 'GERMANY')
   ) y on x.l_orderkey = y.o_orderkey
where
  (
    (s_n_name = 'FRANCE' and c_n_name = 'GERMANY') or
    (s_n_name = 'GERMANY' and c_n_name = 'FRANCE')
  )
) shipping
group by
  supp_nation,
  cust_nation,
  l_year
order by
 supp_nation,
 cust_nation,
 l_year;
''')

Query('q8',
'''select
  o_year,
  sum(case when nation = 'BRAZIL' then volume else 0.0 end) / sum(volume) as mkt_share
from
  (select /*+ mapjoin(ps) */ s_n_name as nation, 
         l_extendedprice * (1- l_discount) as volume, 
         l_orderkey
    from
       partsupp ps join lineitem l on ps.ps_partkey = l.l_partkey
                                   and ps.ps_suppkey = l.l_suppkey
                                   and ps.p_type = 'ECONOMY ANODIZED STEEL'
   ) x
   join
   (select /*+ mapjoin(c) */ o_orderkey, 
           year((o.o_orderdate)) as o_year
    from
        orders o join customer c on o.o_custkey = c.c_custkey
                    and c.c_r_name = 'AMERICA'
                    and year((o.o_orderdate)) in (1995, 1996)
   ) y on x.l_orderkey = y.o_orderkey
group by o_year
order by o_year;
''')

Query('q9',
'''select nation, o_year, sum(amount) as sum_profit
from 
(
select s_n_name as nation,
       year((o.o_orderdate)) as o_year,
       l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
from lineitem li join partsupp ps on ps.ps_partkey = li.l_partkey
                                  and ps.ps_suppkey = li.l_suppkey
                                  and ps.p_name like '%green%'
                 join orders o on li.l_orderkey = o.o_orderkey
) x
group by
  nation,
  o_year
order by
  nation,
  o_year desc;
''')

Query('q10',
'''select
 c_custkey,
 c_name,
 sum(l_extendedprice * (1 - l_discount)) as revenue,
 c_acctbal,
 c_n_name,
 c_address,
 c_phone,
 c_comment
from lineitem li join orders o on li.l_orderkey = o.o_orderkey
                             and li.l_returnflag = 'R'
                             and o.o_orderdate >= cast(unix_timestamp('1993-10-01','yyyy-MM-dd')*1000 as timestamp)
                             and o.o_orderdate < cast(unix_timestamp('1994-01-01','yyyy-MM-dd')*1000 as timestamp)
                  join customer c on o.o_custkey = c.c_custkey
group by
 c_custkey,
 c_name,
 c_acctbal,
 c_phone,
 c_n_name,
 c_address,
 c_comment
order by
 revenue desc
limit 20;
''')

Query('q11',
'''select ps_partkey, value
from
(
 select ps_partkey, sum(ps_supplycost * ps_availqty) as value
 from partsupp
 where s_n_name = 'GERMANY'
 group by ps_partkey
) x join
(
 select sum(ps_supplycost * ps_availqty) * 0.0001 as fraction
 from partsupp
 where s_n_name = 'GERMANY'
) y
where value > fraction
order by
 value desc;
'''),

Query('q11.w',
'''select ps_partkey, value
from
(
select ps_partkey, value,
   sum(value) * 0.0001 over(order by value desc) as fraction 
from
(
 select ps_partkey, sum(ps_supplycost * ps_availqty) as value
 from partsupp
 where s_n_name = 'GERMANY'
 group by ps_partkey
) x
) y
where value > fraction
;
''')

Query('q12',
'''select
 l_shipmode,
 sum(case
     when o_orderpriority ='1-URGENT'
        or o_orderpriority ='2-HIGH'
     then 1
     else 0
     end) as high_line_count,
  sum(case
      when o_orderpriority <> '1-URGENT'
           and o_orderpriority <> '2-HIGH'
      then 1
      else 0
      end) as low_line_count
from orders o join lineitem li on o.o_orderkey = li.l_orderkey
                              and li.l_shipmode in ('MAIL', 'SHIP')
                              and li.l_commitdate < li.l_receiptdate
                              and li.l_shipdate < li.l_commitdate
                              and li.l_receiptdate >= cast(unix_timestamp('1994-01-01','yyyy-MM-dd')*1000 as timestamp)
                              and li.l_receiptdate < cast(unix_timestamp('1995-01-01','yyyy-MM-dd')*1000 as timestamp)
group by
  l_shipmode
order by
  l_shipmode
;
''')

Query('q13',
'''select
  c_count, count(*) as custdist
from (
  select c_custkey, count(o_orderkey) as c_count
  from customer c left outer join orders o on c.c_custkey = o.o_custkey
                                          and o.o_comment not like '%special%requests%'
  group by c_custkey
) c_orders
group by
 c_count
order by
 custdist desc,
 c_count desc;
''')

Query('q14',
'''select
  100.00 * sum(case
               when p_type like 'PROMO%'
               then l_extendedprice*(1-l_discount)
               else 0.0
               end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
  lineitem li join part p on li.l_partkey = p.p_partkey 
                          and li.l_shipdate >= cast(unix_timestamp('1995-09-01','yyyy-MM-dd')*1000 as timestamp)
                          and li.l_shipdate < cast(unix_timestamp('1995-10-01','yyyy-MM-dd')*1000 as timestamp)
;
''')

Query('q15',
'''
select
 s_suppkey,
 s_name,
 s_address,
 s_phone,
 total_revenue
from supplier s join revenue r on s.s_suppkey = r.supplier_no 
                join (select max(total_revenue) as m from revenue) mr on r.total_revenue = mr.m
order by
 s_suppkey;
''',
'''create view revenue(supplier_no, total_revenue) as
select l_suppkey as supplier_no,
         sum(l_extendedprice * (1 - l_discount)) as total_revenue
  from lineitem
  where l_shipdate >= cast(unix_timestamp('1996-01-01','yyyy-MM-dd')*1000 as timestamp)
         and l_shipdate < cast(unix_timestamp('1996-04-01','yyyy-MM-dd')*1000 as timestamp)
  group by l_suppkey
;
''',
'''drop view revenue;
''')

Query('q16',
'''
select
 p_brand,
 p_type,
 p_size,
 count(distinct ps_suppkey) as supplier_cnt
from partsupp ps left outer join supplier s on ps.ps_suppkey = s.s_suppkey and
                           ps.p_brand <> 'Brand#45' and
                           ps.p_type not like 'MEDIUM POLISHED%' and
                           ps.p_size in (49,14,23,45,19,3,36,9) and
                           s.s_comment like '%Customer%Complaints%'
where s_suppkey is null
group by
 p_brand,
 p_type,
 p_size
order by
 supplier_cnt desc,
 p_brand,
 p_type,
 p_size
limit 100;
''', None, None,
'''
-- get rid of part join
-- can get rid of not in clause also, because supplier info is also denormalized.
- but convert to semijoin with supplier
''')

Query('q17',
'''
select sum(l_extendedprice) / 7.0 as avg_yearly
from (
     select l_partkey, 0.2 * avg(l_quantity) as avgQ
     from lineitem
     group by l_partkey
     ) a join 
     (
     select l_partkey, l_quantity, l_extendedprice
     from lineitem li join part p on li.l_partkey = p.p_partkey
                        and p.p_brand = 'Brand#23'
                        and p.p_container = 'MED BOX'
     ) b on a.l_partkey = b.l_partkey
where b.l_quantity < avgQ
;
''',
None, None, '''-- convert correlated subquery to join''')

Query('q17.w',
'''
select sum(l_extendedprice) / 7.0 as avg_yearly
from (
select l_partkey, l_quantity, l_extendedprice,
        avg(l_quantity) over(partition by l_partkey) as avgQ
from lineitem li join part p on li.l_partkey = p.p_partkey
                        and p.p_brand = 'Brand#23'
                        and p.p_container = 'MED BOX'
) a
where l_quantity < 0.2 * avgQ
;
''')

Query('q18',
'''
select
 c_name,
 c_custkey,
 o_orderkey,
 o_orderdate,
 o_totalprice,
 sum(l_quantity)
from
  (select l_orderkey
   from lineitem 
   group by l_orderkey
   having sum(l_quantity) > 300
  ) a join orders o on a.l_orderkey = o.o_orderkey
      join customer c on o.o_custkey = c.c_custkey
      join lineitem li on li.l_orderkey = o.o_orderkey
group by
 c_name,
 c_custkey,
 o_orderkey,
 o_orderdate,
 o_totalprice
order by
 o_totalprice desc,
 o_orderdate
limit 100;
''')

Query('q19',
'''
select
  sum(l_extendedprice * (1 - l_discount) ) as revenue
from lineitem li join part p on li.l_partkey = p.p_partkey 
where             (
                    (
                     p_brand = 'Brand#12'
                     and p_container in ('SM CASE', 'SM BOX', 'SM PKG', 'SM PACK')
                     and l_quantity >= 1 and l_quantity <= 1 + 10
                     and p_size between 1 and 5
                     and l_shipmode in ('AIR', 'AIR REG')
                     and l_shipinstruct = 'DELIVER IN PERSON'
                    )
                    or
                    (
                     p_brand = 'Brand#23'
                     and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                     and l_quantity >= 10 and l_quantity <= 10 + 10
                     and p_size between 1 and 10
                     and l_shipmode in ('AIR', 'AIR REG')
                     and l_shipinstruct = 'DELIVER IN PERSON'
                    )
                    or
                    (
                     p_brand = 'Brand#34'
                     and p_container in ('LG CASE', 'LG BOX', 'LG PKG', 'LG PACK')
                     and l_quantity >= 20 and l_quantity <= 20 + 10
                     and p_size between 1 and 15
                     and l_shipmode in ('AIR', 'AIR REG')
                     and l_shipinstruct = 'DELIVER IN PERSON'
                    )
                   )
;
''')

Query('q20',
'''
select
  s_name,
  s_address
from partsupp ps join
     (
     select l_partkey, l_suppkey, 0.5 * sum(l_quantity) as sumQ
     from lineitem
     where l_shipdate >= cast(unix_timestamp('1994-01-01','yyyy-MM-dd')*1000 as timestamp) and
           l_shipdate < cast(unix_timestamp('1995-01-01','yyyy-MM-dd')*1000 as timestamp)
     group by l_partkey, l_suppkey
     ) li on ps.ps_partkey = li.l_partkey and ps.ps_suppkey = li.l_suppkey and
             ps.s_n_name = 'CANADA' and ps.p_name like 'forest%'
where ps_availqty > sumQ
order by s_name
;
''',
None, None, '''
-- got rid join of supplier, nation, partsupp, part with partsupp
-- correlation to join
''')

Query('q20.w',
'''
select
  s_name,
  s_address
from
(
select 
  s_name,
  s_address,
  ps_availqty,
  sum(l_quantity) over (partition by l_partkey, l_suppkey) as sumQ
from partsupp ps join lineitem li on  ps.ps_partkey = li.l_partkey and 
             ps.ps_suppkey = li.l_suppkey and
             ps.s_n_name like 'CANADA' and ps.p_name like 'forest%' and
             li.l_shipdate >= cast(unix_timestamp('1994-01-01','yyyy-MM-dd')*1000 as timestamp) and
             li.l_shipdate < cast(unix_timestamp('1995-01-01','yyyy-MM-dd')*1000 as timestamp)
) a
where ps_availqty > sumQ
order by s_name
;
''')

Query('q21',
'''
select s_name, count(*) as numwait
from
(
select li.l_suppkey, li.l_orderkey
from lineitem li join orders o on li.l_orderkey = o.o_orderkey and
                      o.o_orderstatus = 'F'
     join
     (
     select l_orderkey, count(distinct l_suppkey) as cntSupp
     from lineitem
     group by l_orderkey
     ) l2 on li.l_orderkey = l2.l_orderkey and 
             li.l_receiptdate > li.l_commitdate and
             l2.cntSupp > 1
) l1 join 
(
select l_orderkey, count(distinct l_suppkey) as cntSupp
from lineitem
where l_receiptdate > l_commitdate
group by l_orderkey
) l3 on l1.l_orderkey = l3.l_orderkey and 
             l3.cntSupp = 1
 join supplier s on l1.l_suppkey = s.s_suppkey
group by
 s_name
order by
 numwait desc,
 s_name
limit 100;
''')

Query('q21.w',
'''
select s_name, count(*) as numwait
from
(
select *
from
(
select
  l_suppkey, l_orderkey,
  min(l_suppkey) over(partition by l_orderkey) as minS,
  max(l_suppkey) over(partition by l_orderkey) as maxS,
  min(case when l_receiptdate > l_commitdate then l_suppkey else 100000000 end) over(partition by l_orderkey) as minS2,
  max(case when l_receiptdate > l_commitdate then l_suppkey else -1 end) over(partition by l_orderkey) as maxS2
from lineitem
) a
where minS != maxS and
      l_suppkey = minS2 and l_suppkey = maxS2
) a join orders o on a.l_orderkey = o.o_orderkey and
                    o.o_orderstatus = 'F'
    join supplier s on a.l_suppkey = s.s_suppkey
group by
 s_name
order by
 numwait desc,
 s_name
limit 100;
''')
