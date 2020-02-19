import argparse

from typing import List
from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.types import StructField, StringType, StructType
from pyspark.sql.functions import col


def fetch_data(
    filename: str,
    encoding: str,
    rowTag: str,
    schema=None
):
    """
    Returns PySpark Dataframe object as a result of one or several files parsing
    To use, please specify data files destinaton (filename) and delimiter chars (separator and escapechar)

    :param filename: path to data files to parse (can be both separate file or collection by * pattern)
    :param encoding: datafile encoding (UTF-8, ASCII, etc)
    :param rowTag:  string to split the records
    :param schema: (optional) dataframe schema to use

    :return PySpark DataFrame object storing the data from given files
    """
    if not (isinstance(rowTag, str) and isinstance(encoding, str)):
        raise ValueError('Both rowTag and encoding should be str')

    pyspark_df = (
        sqlContext
            .read
            .format('xml')
            .option('inferschema', 'false')
            .option('encoding', encoding)
            .options(rowTag=rowTag)
    )
    if schema is not None:
        return pyspark_df.schema(schema).load(filename)
    return pyspark_df.load(filename)


def obtain_schema(
    attributes: List[str],
    dtypes: List[type]
):
    """
    Returns schema object to be attached to PySpark DataFrame

    :param attributes: list of dataframe columns
    :param dtypes: list of column values datatypes
    :return: schema object as a PySpark StructType
    """
    columns_struct_fields = [
        StructField(attr, dtype, True) for attr, dtype in zip(attributes, dtypes)
    ]
    return StructType(columns_struct_fields)


if __name__ == "__main__":
    sc = SparkContext()
    sqlContext = HiveContext(sc)

    parser = argparse.ArgumentParser(
        description='A simple PySpark routine to parse given csv data files and deploy them to Hive'
    )
    parser.add_argument('--file', type=str, dest='file', required=True,
                        help='data file destination (given by pattern name is also valid)')
    parser.add_argument('--encoding', type=str, dest='encoding', required=True,
                        help='charset of given xml file')
    parser.add_argument('--rowTag', type=str, dest='rowTag', required=True,
                        help='separator to treat a row')
    parser.add_argument('--to_table', type=str, dest='to_table', required=True,
                        help='name of Hive db table to store the results of processing')

    args = parser.parse_args()
    file, encoding, row_tag, table_name = args.file, args.encoding, args.rowTag, args.to_table
    # Fetch the data
    data = fetch_data(file, encoding, row_tag).drop('_corrupt_record')
    # Deploy the dataframe data on Hive db
    data.write.mode('overwrite').saveAsTable(table_name)
    # sqlContext.sql("show tables").show()
    data.printSchema()
