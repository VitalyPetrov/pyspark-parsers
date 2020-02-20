import argparse

from typing import List
from pyspark import SparkContext
from pyspark.sql import (
    SQLContext,
    HiveContext
)
from pyspark.sql.types import (
    StructField,
    StringType,
    StructType
)


def fetch_data(
    filename: str,
    separator: str,
    escapechar: str,
    schema=None
):
    """
    Returns PySpark Dataframe object as a result of one or several files parsing
    To use, please specify data files destinaton (filename) and delimiter chars (separator and escapechar)

    :param filename: path to data files to parse (can be both separate file or collection by * pattern)
    :param separator: char to split the columns
    :param escapechar: char to split the rows
    :param schema: (optional) dataframe schema to use

    :return PySpark DataFrame object storing the data from given files
    """
    if not (isinstance(separator, str) and isinstance(escapechar, str)):
        raise ValueError('Both separator and escapechar should be str')

    pyspark_df = (
        sqlContext
        .read
        .format('com.databricks.spark.csv')
        .option('header', 'true')
        .option('inferschema', 'false')
        .option('delimiter', separator)
        .option('escape', escapechar)
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
    sqlContext = SQLContext(sc)

    parser = argparse.ArgumentParser(
        description='A simple PySpark routine to parse given csv data files and deploy them to Hive'
    )
    parser.add_argument('--files', type=str, dest='files', required=True,
                        help='data files destination (given by pattern name is also valid)')
    parser.add_argument('--sep', type=str, dest='sep',
                        required=True, help='char to split the columns')
    parser.add_argument('--esc', type=str, dest='escape',
                        required=True, help='char to split the rows')
    parser.add_argument('--to_table', type=str, dest='to_table', required=False,
                        help='name of Hive db table to store the results of processing')
    args = parser.parse_args()
    files, sep, escape, table_name = args.files, args.sep, args.escape, args.to_table
    # Fetch the data
    data = fetch_data(files, separator=sep, escapechar=escape)

    # Deploy the dataframe data on Hive db
    # it works only if HiveContext is initialized on your machine
    if isinstance(sqlContext, HiveContext):
        data.write.mode('overwrite').saveAsTable(table_name)

    data.printSchema()