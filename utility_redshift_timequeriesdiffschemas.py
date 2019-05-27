oneDim_SQL ="""
set enable_result_cache_for_session to off;
SET search_path TO {};

select sum(lo_extendedprice*lo_discount) as revenue
from lineorder, dwdate
where lo_orderdate = d_datekey
and d_year = 1997 
and lo_discount between 1 and 3 
and lo_quantity < 24;
"""

twoDim_SQL="""
set enable_result_cache_for_session to off;
SET search_path TO {};

select sum(lo_revenue), d_year, p_brand1
from lineorder, dwdate, part, supplier
where lo_orderdate = d_datekey
and lo_partkey = p_partkey
and lo_suppkey = s_suppkey
and p_category = 'MFGR#12'
and s_region = 'AMERICA'
group by d_year, p_brand1
"""

drill_SQL = """
set enable_result_cache_for_session to off;
SET search_path TO {};

select c_city, s_city, d_year, sum(lo_revenue) as revenue 
from customer, lineorder, supplier, dwdate
where lo_custkey = c_custkey
and lo_suppkey = s_suppkey
and lo_orderdate = d_datekey
and (c_city='UNITED KI1' or
c_city='UNITED KI5')
and (s_city='UNITED KI1' or
s_city='UNITED KI5')
and d_yearmonth = 'Dec1997'
group by c_city, s_city, d_year
order by d_year asc, revenue desc;
"""


oneDimSameDist_SQL ="""
set enable_result_cache_for_session to off;
SET search_path TO {};

select lo_orderdate, sum(lo_extendedprice*lo_discount) as revenue  
from lineorder, part
where lo_partkey  = p_partkey
group by lo_orderdate
order by lo_orderdate
"""

def compareQueryTimes(schema):
    queryTimes  =[] 
    for i,query in enumerate([oneDim_SQL, twoDim_SQL, drill_SQL, oneDimSameDist_SQL]):
        t0 = time()
        q = query.format(schema)
        %sql $q
        queryTime = time()-t0
        queryTimes.append(queryTime)
    return pd.DataFrame({"query":["oneDim","twoDim", "drill", "oneDimSameDist"], "queryTime_"+schema:queryTimes}).set_index('query')
    
noDistQueryTimes = compareQueryTimes("nodist")
distQueryTimes   = compareQueryTimes("dist") 

queryTimeDF =noDistQueryTimes.join(distQueryTimes)
queryTimeDF.plot.bar()
plt.show()