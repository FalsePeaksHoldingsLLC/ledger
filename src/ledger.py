from pyspark.sql import SparkSession
from lib.tables.amex_table import AmexTable
from lib.tables.tags_table import TagsTable
from lib.targets.amex_target import AmexTarget
from lib.targets.tags_target import TagsTarget
from lib.utils.sql_reader import SQLReader

class Ledger(object):
    ROOT_PATH='/root/data/staging/'

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )
    amexTarget = AmexTarget(target=ROOT_PATH)
    tagsTarget = TagsTarget(target=ROOT_PATH)

    w = tagsTarget.read_csv(spark=spark, table=TagsTable())
    amexTarget.read_csv(spark=spark, table=AmexTable())

    queries = [
        ('amex', True),
        ('tags', True),
        ('enrich', False)
    ]
    data_frames=[]
    for query_name, tempView in queries:
        query = SQLReader().read_sql(query_name=query_name)
        data_frame = spark.sql(query)
        if tempView:
            data_frame.createTempView(query_name + '_view')
        data_frames.append(data_frame)
    results = data_frames[-1]

    results.write.csv(path=ROOT_PATH + 'ledger', header=True)

if __name__ == '__main__':
    Ledger()