#import all necessary packages
# -*- coding: utf-8 -*-
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime 
from pyspark.sql.functions import *
from pyspark.sql.functions import regexp_replace, when
import pyspark.sql.functions as F
import os
import shutil

##*********************************************************************##
## NOTE: Please refer to problem statements from challenge.htm file    ##
## found under instruction folder for detailed requirements. We have   ##
## defined placeholder functions where you need to add your code that  ##
## may solve one or more of the the problem statements. Within the     ## 
## functions we have added DEFAULT code which when executed without    ##
## any modification would create empty dataframe. This approach will   ## 
## enable you to test your code intermediate without worrying about    ## 
## syntax failure outside of your code. To SOLVE the problem, you are  ##
## expected to REPLACE the code which creates dummy dataframe with     ##
## your ACTUAL code.                                                   ##
##*********************************************************************##
## NOTE: We have also added code to print sample data to console in    ##
## each of the functions to help you visualize your intermediate data  ##
##*********************************************************************##

def main():
    """ Main driver program to control the flow of execution.
        NOTE: Please DO NOT change anything here as it will impact execution of the program.
    """
    #Clean the output files for fresh execution
    outputfile_cleanup()
  
    #Get a new spark session
    spark = get_spark_session()

    #Define schema of input file 
    schema = StructType([StructField("latitute", DoubleType(), True),
                    StructField("longitude",  DoubleType(), True),
                    StructField("description",  StringType(), True),
                    StructField("zip",  IntegerType(), True),
                    StructField("title",  StringType(), True),
                    StructField("timestamp",  StringType(), True),
                    StructField("township",  StringType(), True),
                    StructField("address",  StringType(), True),
                    StructField("e",  StringType(), True)]
                )
    
    #Set up the files path and names.
    cwd = os.getcwd()
    dirname = os.path.dirname(cwd)
    input_path = "file:///home/labuser/Desktop/Project/wings-emer-challenge/input/" 
    output_path = "file:///home/labuser/Desktop/Project/wings-emer-challenge/output/"
    filename = '911.csv'
    final_file = "emergency_call_data"
    calltype_stats = "call_type_data"
    cardiac_stats = "cardiac_call_data"
    traffic_night_stats = "traffic_night_data"
    
    # Execute ETL pipeline
    # EXTRACT the data
    # PROBLEM 1 
    df = extract_data(spark,input_path,filename,schema)

	# TRANSFORM the data
    # PROBLEM 2 - 6
    cols_to_drop = ['description','e','timestamp','title']
    transformed_df = data_transform(spark,df,cols_to_drop)

    # ANALYSIS of data
    # PROBLEM 7 - 9
    type_df,cardiac_df,night_df = data_analytics(spark,transformed_df)
    
    # LOAD the data
    # PROBLEM 10
    load_data(transformed_df,output_path,final_file)
    load_data(type_df,output_path,calltype_stats)
    load_data(cardiac_df,output_path,cardiac_stats)
    load_data(night_df,output_path,traffic_night_stats)

    #Stop spark session
    spark.stop()
#**************************************************************************#
# PROBLEM 1 - Extract the input csv data from local storage to a dataframe #
#**************************************************************************#
def extract_data(spark,input_path,filename,schema):
    """Extract data from storage into a dataframe. The input path and filename 
    have been already set for you. 
    
    Parameters:
    spark: SparkSession
    input_path: Path of the input file
    filename: Name of the input file
    schema: schema to be used while reading the input file

    Returns: New Dataframe
    """
    print("---------------------")
    print("Starting extract_data")
    print("---------------------")
        
    #Write your code below this line
    #File path is already set for you
    path = input_path + filename
    df =  spark.read.option("header", "True").format("csv").schema(schema).load(path) #Replace this line with your actual code
    #Write your code above this line

    df.show(5)
    df.printSchema()
    return df

#*********************************#
# PROBLEM 2 through 6 - Transform #
#*********************************#
def data_transform(spark,df,cols_to_drop):
    
    """Transform data 
        
    Parameters:
    spark: SparkSession
    data: Dataframe to be transformed
    cols_to_drop: Columns that need to be dropped as part of the transformation

    Returns:
    New transformed Dataframe
    """
    print("-----------------------")
    print("Starting data_transform")
    print("-----------------------")
    
    #**************************************************************************************#
    # PROBLEM 2 - Cast "timestamp" column's datatype from string to timestamp.             #
    # PROBLEM 3 - Extract date attributes like year, month, day of the month and hour      #
    # from "timestamp" column.                                                             #
    # PROBLEM 4 - Create new columns "type and "subtype" from "title" column.              #
    # PROBLEM 5 - Clean "subtype" column by removing "-" character.                        #
    # PROBLEM 6 - Drop columns from the dataframe. In this case columns to drop are        #
    # "description","e","title","timestamp".                                               #
    #**************************************************************************************#
    
    #Write your code below this line
    #NOTE - The final dataframe returned should contain all transformation stated above.
    df = df.withColumn('datetime',F.coalesce(F.unix_timestamp('timestamp', 'MM-dd-yyyy HH:MM:SS'),F.unix_timestamp('timestamp', 'MM/dd/yy HH:MM')))
    df.show(1) 
    df = df.withColumn('datetime',F.coalesce(F.to_timestamp('datetime')))
    df.show(1) 
    df = df.withColumn('date', F.split(df['datetime'], ' ').getItem(0)) 
    df = df.withColumn('time', F.split(df['datetime'], ' ').getItem(1)) 
    df.show(1)
    df = df.withColumn('year', F.split(df['date'], '-').getItem(0)) \
       .withColumn('month', F.split(df['date'], '-').getItem(1)) \
       .withColumn('day', F.split(df['date'], '-').getItem(2)) \
       .withColumn('hour', F.split(df['time'], ':').getItem(0)) 
    df.show(1)
    df = df.withColumn('type', F.split(df['title'], ':').getItem(0))
    df = df.withColumn('subtype', F.split(df['title'], ':').getItem(1))
    df.show(1)
    df = df.withColumn('subtype', regexp_replace('subtype', '-', ''))
    df = df.select("latitute","longitude","zip","township","address","year","month","day","hour","type","subtype")
    df.show(1)
    #Write your code above this line
        
    #Below make sure you assign and return the final transformed dataframe. 
    transformed_df = df
    transformed_df.show(1)
    transformed_df.printSchema()
    return transformed_df

#*********************************#
# PROBLEM 7 through 9 - Analytics #
#*********************************#
def data_analytics(spark,transformed_df):
    """ Create analtical reports
    
    Parameters:
    spark: SparkSession
    data: Transformed Dataframe
    
    Returns:
    3 New Dataframe
    """ 
    print("-----------------------") 	
    print("Starting data_analytics")
    print("-----------------------")
    
    #**************************************************************************************#
    # PROBLEM 7 - Create a report that summarizes the count of emergency calls grouped by  #
    # type and subtype.  Order the records by "type" ascending and "count" decending       #     
    #**************************************************************************************#
    #Write your code below this line
    type_df = transformed_df.groupBy("type","subtype").count().orderBy(col("type").asc(),col("count").desc()) #Replace this line with your actual code
    #Write your code above this line
    print("call_type_info")
    type_df.show(1)
    
    #**************************************************************************************#
    # PROBLEM 8 - Create a report which summarizes the count of emergency calls for each   #
    # township where the type of incident is cardiac. Order the records in descending order#
    # of count.                                                                            #
    #**************************************************************************************#
    #Write your code below this line
    #cardiac_df = transformed_df.groupBy("township").filter(F.col('subtype') == 'CARDIAC EMERGENCY').count().orderBy(col("count").desc()) 
    cardiac_df = transformed_df.groupBy("township").count().sort(col("count").desc())
    #Write your code above this line
    print("cardiac_info")
    cardiac_df.show(5)

    #**************************************************************************************#
    # PROBLEM 9 - Create a report which summarizes the count of traffic incidents between  #
    # 12 am and 6 am (0 to 6 hours). Order the records in descending order of count.       # 
    #**************************************************************************************#
    #Write your code below this line
    night_df = transformed_df.groupBy("subtype").count().sort(col("count").desc()) #Replace this line with your actual code
   
    #Write your code above this line
    print("Night_info")
    night_df.show(1)  
    
    return type_df,cardiac_df,night_df

#********************************************************************#
# PROBLEM 10 - Load/Save dataframes to local storage as parquet files#
#********************************************************************#
def load_data(df,output_path,filename):
    """ Load/Save the dataframe to file 
    
    Parameters:
    data: Dataframe that needs to be saved as file
    output_path: Path where the file needs to be saved
    filename: Name of the file

    """
    print("Starting load_Data: ",filename)
    if (df.count() != 0):
        #File path is already set for you
        path = output_path + filename
        #Write your code below this line
        df.coalesce(1).write.format("parquet").mode("overwrite").save(path)
        #Write your code above this line
    else:
        print("Empty dataframe, hence cannot save the data") 



def outputfile_cleanup():
    """ Clean up the output files for a fresh execution.
        This is executed every time a job is run. 
        NOTE: Please DO NOT change anything here as it will impact execution of the program.
    """
    path = "../output/"
    if (os.path.isdir(path)):
        try:
            shutil.rmtree(path)  
            print("% s removed successfully" % path)
            os.mkdir(path)  
        except OSError as error:  
            print(error)  
    else:
        print("The directory does not exist. Creating..% s", path)
        os.mkdir(path)
            
def get_spark_session():
    """ Create new spark session. Please DO NOT change anything here.
        Returns: New spark session
    """
    spark = (SparkSession.builder
                         .appName("video sales")
                         .master("local")
                         .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark

#Dummy schema for the empty dataframe beign created initially. Please ignore this.
dummy_schema = StructType([StructField("Dummy", StringType(), True)])

if __name__ == "__main__":
	main()


