from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructType,
    StructField,
)

class AmexTable:
    @property
    def name(self):
        return 'amex'

    @property
    def delimiter(self):
        return ','

    @property
    def header(self):
        return True

    @property
    def schema(self):
        return StructType(fields=[
            StructField(name='date',            dataType=DateType(),    nullable=False),
            StructField(name='description',     dataType=StringType(),  nullable=True),
            StructField(name='amount',          dataType=DoubleType(),  nullable=False)
        ])