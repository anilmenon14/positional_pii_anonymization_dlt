# Databricks notebook source
%pip install -q faker mimesis ff3 presidio_analyzer presidio_anonymizer tabulate spacy==3.7.5 '/Workspace/Applications/PII_Contextual_Anonymizer/files/pipeline_code/pii_analyze_anonymize-0.1-py3-none-any.whl'
# COMMAND ----------
import subprocess
import dlt
import pyspark.sql.functions as F
from pii_analyze_anonymize import PIIScanner, PIIAnonymizer

@dlt.table(
    name="customers_pii_analyzed",
    comment="Data with PII analyzed"
)
def customers_pii_analyzed():
    # Ensure spacy model is downloaded
    command = ['python', '-m', 'spacy', 'download', 'en_core_web_lg']
    subprocess.run(command)

    pii_scanner = PIIScanner(spark=spark, broadcasted_analyzer=None)
    #known_PII_Cols = ['firstname','lastname']
    known_PII_Cols = spark.conf.get("known_pii_cols").split(',') # This is being passed in DLT config as a comma separated list and split here

    #cols_to_exclude = ['id','operation','operation_date','part1','part2','part3','file_path','file_name','file_size','file_block_start','file_block_length','file_modification_time','processing_time']
    cols_to_exclude = spark.conf.get("cols_to_exclude").split(',') # This is being passed in DLT config as a comma separated list and split here

    df = dlt.read_stream('customers_with_pii')
    
    analyzedDF = pii_scanner.scan_dataframe_batchanalyzer_partitioned(df, excludeCols=cols_to_exclude, knownPIICols=known_PII_Cols, parallelism_factor=32
                                                                              , monotonicIDCheck=False, monotonicIDDebug=False)
    
    return analyzedDF


@dlt.table(
    name="customers_pii_anonymized",
    comment="Bronze data after having PII anonymized"
)
def customers_pii_anonymized():
    pii_anonymizer = PIIAnonymizer(spark=spark)
    
    df = dlt.read_stream("customers_pii_analyzed")
    anonymizedDF = pii_anonymizer.anonymize_scanned_table_streaming(df,parallelism_factor = 32)

    return anonymizedDF