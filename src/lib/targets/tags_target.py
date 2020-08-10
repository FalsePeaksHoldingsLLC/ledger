from os.path import join

class TagsTarget:
    def __init__(self, target):
        self._target = target

    def _path(self, table_name):
        return join(self._target, table_name)

    def read_csv(self, spark, table):
        path = self._path(table.name)
        data_frame = spark.read.csv(
            path=path, 
            header=table.header,
            schema=table.schema,
            sep=table.delimiter
        )
        data_frame.createTempView(name=table.name)
        return data_frame