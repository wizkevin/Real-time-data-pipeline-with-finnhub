import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp,udf,avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType
import uuid


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS market
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS market.trades (
        uuid UUID PRIMARY KEY,
        symbol TEXT,
        trade_conditions TEXT,
        price DOUBLE,
        volume DOUBLE,
        trade_timestamp TIMESTAMP,
        ingest_timestamp TIMESTAMP,
        );
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    trade_id = kwargs.get('uuid')
    symbol = kwargs.get('symbol')
    trade_conditions = kwargs.get('trade_conditions')
    price = kwargs.get('price')
    volume = kwargs.get('volume')
    trade_timestamp = kwargs.get('trade_timestamp')
    ingest_timestamp = kwargs.get('ingest_timestamp')

    try:
        session.execute("""
            INSERT INTO market.trades(uuid, symbol, trade_conditions, price, volume, 
                trade_timestamp, ingest_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (trade_id, symbol, trade_conditions, price, volume,
              trade_timestamp, ingest_timestamp))
        logging.info(f"Data inserted for {trade_id} {symbol}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'finnhub') \
            .option('startingOffsets', 'latest') \
            .load()
        logging.info("kafka dataframe created successfully")
        print(spark_df)
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['cassandra'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None

# Define a User Defined Function (UDF) to generate UUIDs
def make_uuid():
    return udf(lambda: str(uuid.uuid1()), StringType())()

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        # StructField("id", StringType(), False),
        StructField("s", StringType(), False),
        StructField("c", StringType(), False),
        StructField("p", DoubleType(), False),
        StructField("v", DoubleType(), False),
        StructField("t", LongType(), False)
        # StructField("ingest_timestamp", StringType(), False)
    ])

    parsed_df = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
        
    update_df = parsed_df.withColumn("uuid", make_uuid()) \
        .withColumnRenamed("c", "trade_conditions") \
        .withColumnRenamed("p", "price") \
        .withColumnRenamed("s", "symbol") \
        .withColumnRenamed("t", "trade_timestamp") \
        .withColumnRenamed("v", "volume") \
        .withColumn("trade_timestamp", (col("trade_timestamp") / 1000).cast(TimestampType())) \
        .withColumn("ingest_timestamp", current_timestamp())
        
    print(update_df)

    return update_df


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            # insert_data(session)

            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'market')
                               .option('table', 'trades')
                               .start())

            streaming_query.awaitTermination()