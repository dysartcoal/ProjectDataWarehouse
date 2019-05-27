def loadTables(schema, tables):
    loadTimes = []
    SQL_SET_SCEMA = "SET search_path TO {};".format(schema)
    %sql $SQL_SET_SCEMA
    
    for table in tables:
        SQL_COPY = """
copy {} from 's3://awssampledbuswest2/ssbgz/{}' 
credentials 'aws_iam_role={}'
gzip region 'us-west-2';
        """.format(table,table, DWH_ROLE_ARN)

        print("======= LOADING TABLE: ** {} ** IN SCHEMA ==> {} =======".format(table, schema))
        print(SQL_COPY)

        t0 = time()
        %sql $SQL_COPY
        loadTime = time()-t0
        loadTimes.append(loadTime)

        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
    return pd.DataFrame({"table":tables, "loadtime_"+schema:loadTimes}).set_index('table')

#-- List of the tables to be loaded
tables = ["customer","dwdate","supplier", "part", "lineorder"]

#-- Insertion twice for each schema (WARNING!! EACH CAN TAKE MORE THAN 10 MINUTES!!!)
nodistStats = loadTables("nodist", tables)
distStats = loadTables("dist", tables)

#-- Plotting of the timing results
stats = distStats.join(nodistStats)
stats.plot.bar()
plt.show()
