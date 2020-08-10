from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
)

class TagsTable:
    @property
    def name(self):
        return 'tags'

    @property
    def delimiter(self):
        return ','

    @property
    def header(self):
        return True

    @property
    def schema(self):
        return StructType(fields=[
            StructField(name='type',    dataType=StringType(),  nullable=False),
            StructField(name='lookup',  dataType=StringType(),  nullable=False)
        ])