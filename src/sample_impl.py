from pyspark.sql.functions import *
from pyspark.sql.types import *

ENRICHED_EVENT_SCHEMA = StructType([
    StructField("ppid", IntegerType(), True),
    StructField("marker", StringType(), True),
    StructField("vendorId", StringType(), True),
    StructField("ts", TimestampType(), True)
])

AGENCY_EVENT_SCHEMA = StructType([
    StructField("ppid", IntegerType(), True),
    StructField("marker", StringType(), True),
    StructField("ts", TimestampType(), True)
])

VENDOR_EVENT_SCHEMA = StructType([
    StructField("ppid", IntegerType(), True),
    StructField("marker", StringType(), True),
    StructField("date", TimestampType(), True),
    StructField("vendor_id", StringType(), True)
])


def load_agency_events(spark, agency_events_path):
    """Parse a Agency Events CSV into a DataFrame"""

    # YOUR CODE HERE
    df = spark.read.format("csv").option("header","true").load(agency_events_path)

    # cast Types
    df.createOrReplaceTempView("AgencyEvents")
    dfCasted = spark.sql("SELECT INT(ppid), STRING(marker), TIMESTAMP(ts) from AgencyEvents")

    return dfCasted



def load_vendor_events(spark, vendor_events_path):
    """Parse a Vendor Events CSV into a DataFrame"""

    # YOUR CODE HERE
    df = spark.read.format("csv").option("header","true").load(vendor_events_path)

    # cast Types - ppid,marker,date,vendor_id
    df.createOrReplaceTempView("VendorEvents")
    dfCasted = spark.sql("SELECT INT(ppid), STRING(marker), TIMESTAMP(date), STRING(vendor_id) from VendorEvents")

    return dfCasted


def enrich_events(spark, agency_events_path, vendor_events_path):
    agency_events = load_agency_events(spark, agency_events_path)
    vendor_events = load_vendor_events(spark, vendor_events_path)

    # YOUR CODE HERE

    vendor_events.createOrReplaceTempView("ve_View")
    tempv0DF = spark.sql("SELECT v.ppid, v.marker, v.vendor_id , v.date, \
            YEAR(v.date) as year, MONTH(v.date) as month, DAY(v.date) as day FROM ve_View v ORDER BY year, month, day ")
    tempv0DF.createOrReplaceTempView("ve_withymdView")
    tempv1DF = spark.sql("SELECT v.ppid, v.marker, v.vendor_id as vendorId, v.date as ts, \
            v.year as year, v.month as month, v.day as day FROM ve_withymdView v ORDER BY year, month, day ")
    tempv1DF.createOrReplaceTempView("tempv1View")
    tempv2DF = spark.sql(" \
            select * from \
               (select *, ROW_NUMBER() \
               OVER (PARTITION BY ppid, marker, year, month ORDER BY year DESC, month DESC, day DESC) \
               AS row_number from tempv1View) \
            temp_view_ranked \
            where row_number = 1 \
            ")
    tempv2DF.createOrReplaceTempView("tempv2View")

    agency_events.createOrReplaceTempView("agencyView")
    tempp2DF = spark.sql("SELECT p.ppid as ppid, p.marker as marker, p.ts as ts, \
            YEAR(p.ts) as year, MONTH(p.ts) as month, DAY(p.ts) as day FROM agencyView p ORDER BY year, month, day ")
    tempp2DF.createOrReplaceTempView("tempp2View")

    enrichedDF = spark.sql("select p.ppid as ppid, \
                           p.marker as marker, \
                           v.vendorId as vendorId, \
                           CASE \
                               WHEN v.ts IS NOT null THEN v.ts \
                               ELSE p.ts \
                           END \
                           as ts \
                           from tempv2View v \
                           RIGHT JOIN tempp2View p \
                           ON (p.ppid == v.ppid and p.marker == v.marker \
                           and  p.month == v.month \
                           ) \
                           order by ppid, marker, ts")

    return enrichedDF