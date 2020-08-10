from __future__ import print_function
import sys
import requests
from pyspark import SparkContext, SQLContext, Row
from pyspark.streaming import StreamingContext


sc = SparkContext(appName="Streaming Twitter Analysis")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 5)
ssc.checkpoint("checkpoint_TwitterApp")
socket_stream = ssc.socketTextStream("127.0.0.1", 9009)
lines = socket_stream.window(60)


def get_sql_context_instance(spark_context):
    if 'sqlContextSingletonInstance' not in globals():
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def process_rdd(rdd):
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(location=w[0], location_count=w[1]))
        locations_df = sql_context.createDataFrame(row_rdd)
        locations_df.registerTempTable("locations")
        # get the top locations from the table using SQL and print them
        location_counts_df = sql_context.sql(
            "select location, location_count from locations order by location_count desc limit 10")
        location_counts_df.show()
        # call this method to prepare top 10 locations DF and send them
        send_df_to_dashboard(location_counts_df)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


def send_df_to_dashboard(df):
    # extract the locations from dataframe and convert them into array
    top_locations = [str(t.location) for t in df.select("location").collect()]
    # extract the counts from dataframe and convert them into array
    location_count = [p.location_count for p in df.select("location_count").collect()]
    # initialize and send the data through REST API
    url = 'http://localhost:9001/updateData'
    request_data = {'label': str(top_locations), 'data': str(location_count)}
    requests.post(url, data = request_data)

def aggregate_counts(new_value, total_sum):
    return sum(new_value) + (total_sum or 0)


locs = lines.map(lambda word: (word.lower(), 1))
locations = locs.updateStateByKey(aggregate_counts)
#locations = locs.reduceByKey(lambda a, b: a + b)
#location_counts_sorted_dstream = locations.transform(lambda foo: foo.sortBy(lambda x: x[0].lower()).sortBy(lambda x: x[1], ascending=False))
#location_counts_sorted_dstream.pprint()

locations.foreachRDD(process_rdd)


ssc.start()
ssc.awaitTermination()
