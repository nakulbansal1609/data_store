from pyspark.sql import SparkSession


import os
import sys



if __name__ == '__main__':
    print('Starting the main function')
    input_file_format = 'parquet'
    if len(sys.argv) == 1:
        print('No argument is passed to the app')
        sys.exit(1)
    else:
        try:
            instance_id = sys.argv[1]
            filepath = sys.argv[2]
            database_name = sys.argv[3]
            table_name = sys.argv[4]
        except IndexError:
            print('Refering to the wrong index of argument')
            sys.exit(1)
    
    spark = SparkSession \
        .builder \
        .appName('SchemaGeneration_'+instance_id) \
        .enableHiveSupport() \
        .getOrCreate()


    sparkDfSchema = spark.read.format(input_file_format) \
                            .load(filepath) \
                            .schema
    print('Schema of the input file')
    print(sparkDfSchema)

    columns = map(lambda x: \
                    "`" + x.name + "`" + ' ' + x.dataType.simpleString() \
                    ,sparkDfSchema)

    # The below code will run if run in python v3.x or above
    if not isinstance(columns, list):
        columns = list(columns)

    # Convert the list into string separated with comma
    columns = ','.join(columns)

    # Define some fixed values for table DDLs
    ddl_string = 'create external table if not exists '
    #ddl_string = 'create table if not exists '
    storage = 'stored as ' + input_file_format + ' '
    partitioning = 'partitioned by (year int,month int,day int) '
    location = "location '" + filepath + "'"
    query = ddl_string + database_name + '.' + table_name \
            + ' (' + columns + ') ' \
            + partitioning + storage + location

    print('HiveQL generated for DDL')
    print(query)
    print('Submitting HiveQL DDL')
    
    spark.sql(query)
    #spark.sql("show databases").show()

    print('Hive External Table is created ' + database_name + '.' + table_name)
                    
    
    print('Finishing Spark Application')
    spark.stop()
    