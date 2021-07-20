import sys
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime
import datetime 
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import smtplib
import pandas as pd


glueContext = GlueContext(SparkContext.getOrCreate())
logger = glueContext.get_logger()

#####Email alert function
def sendEmail(recipients,panas_df):
    emaillist = [elem.strip().split(',') for elem in recipients]
    msg = MIMEMultipart()
    msg['Subject'] = "Deequ-Anomoly Detected"
    msg['From'] = 'annaldas@amazon.com'

    html = """\
    <html>
        <head></head>
            <body>
                {0}
            </body>
    </html> """.format(panas_df.to_html())

    part1 = MIMEText(html, 'html')
    msg.attach(part1)

    #server = smtplib.SMTP('mail-relay.amazon.com')
    server = smtplib.SMTP('localhost')
    server.sendmail(msg['From'], emaillist , msg.as_string())
    server.quit()
#####



# Required Parameters
args = getResolvedOptions(sys.argv, [
    'glueDatabase',
    'glueTables'])

glue_database = args['glueDatabase']
#glue_tables = [x.strip() for x in args['glueTables'].split(',')]
glue_tables =  ",".join(f"'{w}'" for w in args['glueTables'].split(","))

datasource0 = glueContext.create_dynamic_frame.from_catalog(
            database = 'data_quality_db' ,
            table_name = 'constraints_analysis_results', 
            transformation_ctx = "datasource0") 

constraints_analysis_df = datasource0.toDF().filter("entity = 'Dataset'").cache() #convert to Glue DynamicFrame
constraints_analysis_df.createOrReplaceTempView("constraints_analysis_df")

    
#for glue_table in glue_tables:

    #getting the 5 days old date in UTC since s3 data is stored in UTC format.
    #date_previous5thday = datetime.datetime.utcnow() - datetime.timedelta(days = 5)
    #predicate pushdown to pull only subset of data from S3.
    #predicate_pushdown="database={0} and table={1} and year>={2} and month>={3} day>={4}".format(glue_database,glue_table,date_previous5thday.year, date_previous5thday.month, date_previous5thday.day)
    
    #datasource0 = glueContext.create_dynamic_frame.from_catalog(
    #                database = 'data_quality_db' ,
    #                table_name = 'constraints_analysis_results', 
    #                #push_down_predicate = predicate_pushdown
    #                transformation_ctx = "datasource0")#convert to spark DataFrame
    #constraints_analysis_df = datasource0.toDF() #convert to Glue DynamicFrame
    #constraints_analysis_df.createOrReplaceTempView("constraints_analysis_df")
    #glueContext.sql(""" SELECT * from constraints_analysis_df """).show()


#prepare data for anomoloy calculation    
stats_df=glueContext.sql(""" SELECT database, 
                            table,
                            AVG(CASE WHEN rownumber BETWEEN 2 AND 6 THEN value END) AS past_5runs_avg, 
                            MAX(CASE WHEN rownumber = 1 THEN value END) AS latestbatch_runtime FROM 
                                  (SELECT *, ROW_NUMBER() OVER(PARTITION BY database, table ORDER BY year DESC, month DESC, day DESC, hour DESC, min DESC) AS rownumber FROM constraints_analysis_df WHERE table IN ({0})
                                  ) a 
                    GROUP BY database, table  """.format(glue_tables))
                    
######Sample output of above stats_df
'''
database	table		past_5runs_avg	latestbatch_runtime
extract		sales		2000.0           2100
extract		revenue		1000.0 			 900				
'''

stats_df.createOrReplaceTempView("stats_df")

result_df=glueContext.sql(""" SELECT database AS schema, table, latestbatch_runtime, past_5runs_avg, 
CASE WHEN latestbatch_runtime BETWEEN 0.8*COALESCE(past_5runs_avg,latestbatch_runtime) AND 1.2*COALESCE(past_5runs_avg,latestbatch_runtime) THEN 'N' ELSE 'Y' END AS is_anomoly FROM stats_df """)

print("Checking if data frame empty or not")

if not result_df.where("is_anomoly = 'N'").rdd.isEmpty():
    print("Dataframe is not empty")
    logger.info("Found data anomolies;sending email alert")
    panas_df=result_df.where("is_anomoly = 'Y'").toPandas()
    recipients=['annaldas@amazon.com']
    print("Calling sendemail method")
    sendEmail(recipients,panas_df)
    print("returned from email")
    
    


    