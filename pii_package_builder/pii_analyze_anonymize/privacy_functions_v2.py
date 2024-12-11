# Databricks notebook source
def get_selection(selection: list, all_options: list) -> list:
  """Utility to assist with choosing ALL options in a multi-choice widget"""
  if "ALL" in selection:
    return all_options
  else:
    return selection

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.broadcast import Broadcast
from pyspark.sql.functions import asc, col, when, lit, from_json, explode, mean, count, pandas_udf, struct , array, row_number,monotonically_increasing_id,first, collect_list, expr, concat , sha2, current_timestamp
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType, DoubleType, LongType
from pyspark.sql import Window, DataFrame
import pandas as pd
import json
from datetime import date
from presidio_analyzer import AnalyzerEngine, BatchAnalyzerEngine
from presidio_anonymizer import AnonymizerEngine, BatchAnonymizerEngine
from presidio_anonymizer.entities import DictRecognizerResult, RecognizerResult

# Imports needed to dynamically union DataFrames
from functools import reduce

# COMMAND ----------

class PIIScanner:

  def __init__(
    self, spark: SparkSession, broadcasted_analyzer: Broadcast):
      self.spark = spark 
      self.broadcasted_analyzer = broadcasted_analyzer
      self.scan_schema = ArrayType(StructType([
        StructField("entity_type", StringType()),
        StructField("start", IntegerType()),
        StructField("end", IntegerType()),
        StructField("score", DoubleType())
      ]))
      self.results_schema = StructType([
        StructField("column", StringType()),
        StructField("entity_type", StringType()),
        StructField("num_entities", DoubleType()),
        StructField("avg_score", DoubleType()),
        StructField("sample_size", IntegerType()),
        StructField("hit_rate", DoubleType()),
    ])
      print(f"PII Scanner initialized")

  @staticmethod
  def get_all_uc_tables(spark: SparkSession, catalogs: tuple) -> DataFrame:

    sql_clause = None
    print(f"Getting all uc tables in catalogs {', '.join(catalogs)}")
    if len(catalogs) == 1:
      sql_clause = f"table_catalog IN ('{catalogs[0]}')"
    else:
      sql_clause = f"table_catalog IN {catalogs}"
    return (
      spark.sql(f"SELECT * FROM system.information_schema.tables WHERE {sql_clause}")
      .select("table_catalog", 
              "table_schema", 
              "table_name",
              when(col("table_type") == "VIEW", "VIEW").otherwise(lit("TABLE")).alias("table_type"),
              "created", 
              "last_altered").
      orderBy(
        col("table_catalog").asc(), 
        col("table_schema").asc(), 
        col("table_name").asc()))

  @staticmethod
  def analyzer_func(s: pd.Series) -> pd.Series:
    """Function to be vectorized using 'pandas_udf'. 
    This iteration uses a function that 'has' to iterate through the results, which is unavoidable"""
    analyzer = AnalyzerEngine()

    def analyze_text(text: str) -> str:
      """Uses 'entities' and 'language' from global namespace"""
      analyzer_results = analyzer.analyze(text=text, entities=entities, language=language)
      return json.dumps([x.to_dict() for x in analyzer_results])
    
    return s.astype(str).apply(analyze_text)

  @staticmethod
  def batchanalyzer_func(pdf: pd.DataFrame) -> pd.DataFrame:
    """Function to be vectorized using 'pandas_udf'. 
    This version uses columns of data from the original Dataframe and passes back an analyzed Dataframe using BatchAnalyzerEngine """

    originalColumns = pdf.columns.copy()

    # Flexible code block to handle presence or absence of the monotonic_id column if there is a need to confirm integrity of data returned back
    if 'monotonic_id' in originalColumns:
      monotonicIDCol = 'monotonic_id'
      monotonicIDList = pdf[monotonicIDCol].values.copy() # Track the ID list to reincorporate later into DataFrame after analyzer has produced results
      pdf = pdf.drop(columns=[monotonicIDCol])

    analyzer = AnalyzerEngine()
    batch_analyzer = BatchAnalyzerEngine(analyzer_engine=analyzer)

    df_dict = pdf.to_dict(orient="list")
    analyzer_results = batch_analyzer.analyze_dict(df_dict, language="en")
    pdResults = pd.DataFrame(list(analyzer_results),columns=['key','value','recognizer_results'])

    # Convert analyzer results to dictionary where applicable
    pdResults['recognizer_results'] = pdResults['recognizer_results'].apply(
        lambda columnData: [ [result.to_dict() for result in resultList] if len(resultList) > 0 else resultList for resultList in columnData ]
    )

    # Add the monotonic_id information back into the Dataframe at this point (if it originally existed)
    if 'monotonic_id' in originalColumns:
      pdResults = pd.concat([pdResults, pd.DataFrame([{'key': monotonicIDCol,'value': monotonicIDList,'recognizer_results': monotonicIDList}])], ignore_index=True)

    df_exploded = pdResults.explode('recognizer_results') # Explode the recognizer results column so that we can reshape results in next steps using pivot
    df_exploded['order_of_list'] = df_exploded.groupby('key').cumcount() # Provide an arbitrary key to exploded rows to maintain order in the below pivor
    returnDF = df_exploded.pivot(index='order_of_list',columns='key', values='recognizer_results').reset_index(drop=True)
    returnDF.columns = sorted(originalColumns) # Get the results back to same sorting as original columns
    returnDF = returnDF[list(originalColumns)]

    return returnDF

  def scan_dataframe_original_nonpartitioned(self, df: DataFrame) -> DataFrame:
    """This is the base implementation where we iterate through each of the columns to assess using the base AnalyzerEngine class in Presidio.

    Parameters: 
    - df (DataFrame): The input Spark DataFrame to be analyzed.

    Returns:
    - DataFrame: A new DataFrame with the analysis results added.
    """
    
    # Select columns excluding the specified ones
    tempSuffix = "_analysis_tmp"
    
    analyzerengine_udf = pandas_udf(PIIScanner.analyzer_func, returnType=StringType())

    # Apply UDF analysis on each column with a temporary alias (to not collide with original column names)
    analysisResultDF = df.select(
        *df.columns,  # Include all original columns
        *[from_json(analyzerengine_udf(col(c).cast("string")), self.scan_schema).alias(f"{c}{tempSuffix}") for c in df.columns]
    )
    
    # Structuring the analysis results in a new column "pii_analysis_result", where keys are restored to the original column names
    analysisResultDF = analysisResultDF.withColumn(
        "pii_analysis_result", struct(*[col(f"{c}{tempSuffix}").alias(c) for c in df.columns])
    ).drop(*[f"{c}{tempSuffix}" for c in df.columns])  # Dropping the temporary analysis columns
    
    return analysisResultDF

  def scan_dataframe_col_iterator_nonpartitioned(self, df: DataFrame, excludeCols: list = []) -> DataFrame:
    """This is the same as the base implementation where we iterate through each of the columns to assess WITH column exclusion choice available and WITHOUT repartitioning the DataFrame.

    Parameters: 
    - df (DataFrame): The input Spark DataFrame to be analyzed.
    - excludeCols (list): A list of column names to exclude from the analysis. Default is an empty list, meaning no columns are excluded.

    Returns:
    - DataFrame: A new DataFrame with the analysis results added.
    """
    # Select columns excluding the specified ones
    colList = [c for c in df.columns if c not in excludeCols]
    tempSuffix = "_analysis_tmp"
    
    analyzerengine_udf = pandas_udf(PIIScanner.analyzer_func, returnType=StringType())

    # Apply UDF analysis on each column with a temporary alias (to not collide with original column names)
    analysisResultDF = df.select(
        *df.columns,  # Include all original columns
        *[from_json(analyzerengine_udf(col(c).cast("string")), self.scan_schema).alias(f"{c}{tempSuffix}") for c in colList]
    )
    
    # Structuring the analysis results in a new column "pii_analysis_result", where keys are restored to the original column names
    analysisResultDF = analysisResultDF.withColumn(
        "pii_analysis_result", struct(*[col(f"{c}{tempSuffix}").alias(c) for c in colList])
    ).drop(*[f"{c}{tempSuffix}" for c in colList])  # Dropping the temporary analysis columns
    
    return analysisResultDF


  def scan_dataframe_col_iterator_partitioned(self, df: DataFrame, excludeCols: list = [], parallelism_factor:int = 16) -> DataFrame:
    """This is the same as the base implementation where we iterate through each of the columns to assess WITH column exclusion choice available and WITH repartitioning the DataFrame.

    Parameters: 
    - df (DataFrame): The input Spark DataFrame to be analyzed.
    - excludeCols (list): A list of column names to exclude from the analysis. Default is an empty list, meaning no columns are excluded.
    - parallelism_factor (int): The level of parallelism to use during processing. Default is 16, which controls the number of partitions the data is split into for concurrent processing.

    Returns:
    - DataFrame: A new DataFrame with the analysis results added.
    """
    # Select columns excluding the specified ones
    colList = [c for c in df.columns if c not in excludeCols]
    tempSuffix = "_analysis_tmp"

    analyzerengine_udf = pandas_udf(PIIScanner.analyzer_func, returnType=StringType())

    # Apply UDF analysis on each column with a temporary alias (to not collide with original column names)
    analysisResultDF = df.repartition(parallelism_factor).select(
        *[df[c] for c in df.columns],  # Include all original columns
        *[from_json(analyzerengine_udf(col(c).cast("string")), self.scan_schema).alias(f"{c}{tempSuffix}") for c in colList]
    )
    
    # Structuring the analysis results in a new column "pii_analysis_result", where keys are restored to the original column names
    analysisResultDF = analysisResultDF.withColumn(
        "pii_analysis_result", struct(*[col(f"{c}{tempSuffix}").alias(c) for c in colList])
    ).drop(*[f"{c}{tempSuffix}" for c in colList])  # Dropping the temporary analysis columns
    
    return analysisResultDF


  def scan_dataframe_col_stacked_partitioned(self, df: DataFrame, excludeCols: list = [], parallelism_factor: int = 16) -> DataFrame:
    """This function performs computation by stacking and unstacking columns to take advantage of Spark parallel process instead of having to iterate through columns like previous versions (E.g. 'scan_dataframe_col_iterator_partitioned') have to do.
    
    Parameters: 
    - df (DataFrame): The input Spark DataFrame to be analyzed.
    - excludeCols (list): A list of column names to exclude from the analysis. Default is an empty list, meaning no columns are excluded.
    - parallelism_factor (int): The level of parallelism to use during processing. Default is 16, which controls the number of partitions the data is split into for concurrent processing.

    Returns:
    - DataFrame: A new DataFrame with the analysis results added.
    """
    
    # Apply a unique identifier to the original DataFrame to join back to original DataFrame after stacking and unstacking operations done for analysis
    df = df.withColumn("stack_tracking_id", monotonically_increasing_id())
    scanDF = df.drop(*excludeCols)
    colList = scanDF.columns.copy()
    colList.remove("stack_tracking_id")  # Ensure 'stack_tracking_id' is not included in colList for stacking

    # Stacking columns: This creates a two-column output with column names and their corresponding values
    stack_expr = "stack({0}, {1}) as (columnName, content)".format(
        len(colList), ", ".join([f"'{col}', {col}" for col in colList])
    )
    transformed_df = scanDF.selectExpr("stack_tracking_id", stack_expr)

    analyzerengine_udf = pandas_udf(PIIScanner.analyzer_func, returnType=StringType())

    # Apply the UDF on the 'content' column
    analyzerResultsDF = transformed_df.repartition(parallelism_factor).select(
        "stack_tracking_id",
        "columnName",
        from_json(analyzerengine_udf(col('content').cast("string")), self.scan_schema).alias('content')
    )

    # Pivot back to get original columns and UDF results
    analyzerResultsDF = analyzerResultsDF.groupBy("stack_tracking_id").pivot("columnName").agg(first("content"))
    analyzerResultsDF = analyzerResultsDF.select("stack_tracking_id", struct(*colList).alias('pii_analysis_result'))

    # Join the original DataFrame with the analyzer results
    resultDF = df.join(analyzerResultsDF, on="stack_tracking_id", how="inner").drop('stack_tracking_id')

    return resultDF
  

  def scan_dataframe_batchanalyzer_partitioned(self
      , df: DataFrame, excludeCols: list = [], knownPIICols: list = [], parallelism_factor:int = 16
      , monotonicIDCheck: bool = False, monotonicIDDebug: bool = False) -> DataFrame:  
    """
    This function performs data analysis using the BatchAnalyzerEngine class in Presidio.

    Parameters: 
    - df (DataFrame): The input Spark DataFrame to be analyzed.
    - excludeCols (list): A list of column names to exclude from the analysis. Default is an empty list, meaning no columns are excluded.
    - knownPIICols (list): A list of columns which are known to be containing PII in every single row and can be wholly masked, hence excluded from analysis
    - parallelism_factor (int): The level of parallelism to use during processing. Default is 16, which controls the number of partitions the data is split into for concurrent processing.
    - monotonicIDCheck (bool): A flag indicating whether to perform an integrity check against monotonically increasing ID within the data that is synthetically added before UDF transformation and joined back after the UDF transformation. Default is False. If set to True, the function perfomance will be slower than if set to False, due to additional join processing.
    - monotonicIDDebug (bool): A flag indicating whether to produce monotonically increasing ID in the results to visually inspect if monotonic ID matches. Default is False. If set to True, there will not be additional join processing and simply will lead to additional fields in the results to help compare if result lines up contextually to input. If 'monotonicIDCheck' is set to True, setting this parameter to True will have no visible change.
    
    Note: monotonicIDCheck and monotonicIDDebug only work in Batch mode and will fail in streaming mode due to lack of support for monotonically_increasing_id() in streaming.

    Returns:
    - DataFrame: A new DataFrame with the analysis results added.
    """

    if monotonicIDCheck or monotonicIDDebug:
        batchAnalyzerDF = df.withColumn("monotonic_id", monotonically_increasing_id())
    else:
        batchAnalyzerDF = df.select("*") # Purposeful 'copy' creation of DataFrame to be able to flexibly use variable name 'batchAnalyzerDF' without having to change below code.

    # Define return schema of the DataFrame to pass along to UDF definition
    original_schema = batchAnalyzerDF.drop(*(excludeCols+knownPIICols)).schema
    newReturnSchema = StructType([
        StructField(field.name, ArrayType(StructType([StructField('entity_type', StringType(), True), StructField('start', IntegerType(), True), StructField('end', IntegerType(), True), StructField('score', DoubleType(), True)])), field.nullable) 
        if field.name != "monotonic_id" else StructField(field.name, IntegerType(), True) for field in original_schema
    ])

    batchanalyzerengine_udf = pandas_udf(PIIScanner.batchanalyzer_func, returnType=newReturnSchema)
    colList = batchAnalyzerDF.drop(*(excludeCols+knownPIICols)).schema.fieldNames() # List of columns that are going to be passed through analyzer

    if monotonicIDCheck:
      piiResultDF = batchAnalyzerDF.repartition(parallelism_factor).select(batchanalyzerengine_udf(struct(colList)).alias('pii_analysis_result'),col("pii_analysis_result.monotonic_id").alias('monotonic_id'))
      # Reconstructing the 'pii_analysis_result' struct without the 'monotonic_id' field
      colList.remove("monotonic_id")
      piiResultDF = piiResultDF.withColumn("pii_analysis_result"
                    ,struct(
                      struct(*[col(f"pii_analysis_result.{field}").alias(field) for field in colList]).alias('analysis_results')
                      #,array([lit(x) for x in knownPIICols]).alias("known_pii_columns")
                      ,struct(*[lit("Full mask").alias(field) for field in knownPIICols]).alias('known_pii_columns')
                      )
                    )
      resultDF = batchAnalyzerDF.join(piiResultDF, on="monotonic_id", how="inner").drop('monotonic_id')
      return resultDF
    else:
      return batchAnalyzerDF.repartition(parallelism_factor).select(*batchAnalyzerDF.columns,batchanalyzerengine_udf(struct(colList)).alias('pii_analysis_result')) \
                        .withColumn("pii_analysis_result"
                                      ,struct(
                                        struct(*[col(f"pii_analysis_result.{field}").alias(field) for field in colList]).alias('analysis_results')
                                        #,array([lit(x) for x in knownPIICols]).alias("known_pii_columns")
                                        ,struct(*[lit("Full mask").alias(field) for field in knownPIICols]).alias('known_pii_columns')
                                        )
                                      )

  

# COMMAND ----------

class PIIAnonymizer:

    def __init__(
    self, spark: SparkSession):
        self.spark = spark 
        print(f"PII Anonymizer initialized")

    @staticmethod
    def anonymizer_func(pdf: pd.DataFrame) -> pd.Series:
        """Serial anonymizer function"""

        anonymizerEngine = AnonymizerEngine()
        pdf['recognizer_result'] = pdf['recognizer_result'].apply(lambda result: [RecognizerResult(**item) for item in result])
        pdf['anonymizedResult'] = pdf.apply(lambda row: row['originalCol'] if pd.isna(row['originalCol']) 
                                            else anonymizerEngine.anonymize(text=row['originalCol'], analyzer_results=row['recognizer_result']).text 
                                            ,axis=1
                                            )
        return pdf['anonymizedResult']

    @staticmethod
    def batchanonymizer_func(pdf: pd.DataFrame) -> pd.DataFrame:
        """Function to be vectorized using 'pandas_udf'. 
        This version uses columns of data from the original Dataframe and passes back an anonymized Dataframe using BatchAnonymizerEngine """
        
        batch_anonymizer = BatchAnonymizerEngine()

        pdf['recognizer_result'] = pdf['recognizer_result'].apply(lambda result: [[RecognizerResult(**innerlist) for innerlist in item] for item in result])

        pdf['DictRecognizerResult'] =pdf.apply(lambda row: [DictRecognizerResult(key=row['key'],value=row['value'],recognizer_results=row['recognizer_result'])], axis=1)

        # Run anonymize_dict and receive dictionary with key: List[values_of_anonymized]
        pdf['anonymized_data'] = pdf.apply(lambda row : batch_anonymizer.anonymize_dict(analyzer_results=row['DictRecognizerResult']),axis=1)

        # Use key to gather the lists only
        pdf['anonymized_data'] = pdf.apply(lambda row: row['anonymized_data'][row['key']],axis=1)

        pdf['zipped_data'] = pdf.apply(lambda row: list(zip(row['id'], row['anonymized_data'])), axis=1)

        pdf.drop(['value','recognizer_result','DictRecognizerResult','anonymized_data'],axis=1,inplace=True)

        return pdf
    
    def anonymize_scanned_table_batch(self, analyzedDF: DataFrame) -> DataFrame: 
        """If a natural primary ID column does not exist, use monotonically_increasing_id to define an 'id' column and use it below.
        Note: Do not use this function in a streaming job (E.g. Spark Strucutured streaming or Delta Live Tables)as the .pivot() function
        without pivoted columns declared is not supported in batch mode
        """
        # Add temporary buckets to be able to group IDs to be able to run collect_list more effectively later
        num_buckets = 1024
        analyzedDF = analyzedDF.withColumn("tempBucketId", expr(f"id % {num_buckets}"))

        # Extract schema of the analysis result struct to get the column names to anonymize
        analyzedCols = [field.name for field in analyzedDF.schema["pii_analysis_result"].dataType["analysis_results"].dataType.fields]
        knownPIICols = [field.name for field in analyzedDF.schema["pii_analysis_result"].dataType["known_pii_columns"].dataType.fields]
        originalCols = analyzedDF.columns.copy()

        #TO-DO : Try mapInPandas in both the analyzer and anonymizer code to see effect

        dfsToUnion = []
        for analyzedCol in analyzedCols:
            tempDF = analyzedDF.select(analyzedCol, 'tempBucketId', 'id', col(f"pii_analysis_result.analysis_results.{analyzedCol}").alias(f"{analyzedCol}_analyzerresult"))
            dfsToUnion.append(tempDF.groupBy('tempBucketId').agg(collect_list('id').alias('id'), collect_list(analyzedCol).alias('value'),collect_list(f"{analyzedCol}_analyzerresult").alias('recognizer_result')).select('tempBucketId',lit(analyzedCol).alias('key'),'id','value','recognizer_result'))

        # Apply union to stack results of each column on each other as list of each of the columns grouped by bucket ID
        combinedDF = reduce(lambda df1, df2: df1.union(df2), dfsToUnion)

        # Define return schema of the DataFrame to pass along to UDF definition
        newReturnSchema = StructType([StructField('key', StringType(), True)
                                    , StructField('zipped_data'
                                    , ArrayType(StructType([
                                                    StructField("id", LongType(), True),
                                                    StructField("anonymizedData", StringType(), True)
                                                ]), False), True)])
        
        # Run UDF on the data and get a return data of original data zipped with anonymized data
        batchanonymizerengine_udf = pandas_udf(PIIAnonymizer.batchanonymizer_func, returnType=newReturnSchema)
        listOfColsForUDF = combinedDF.columns.copy()
        listOfColsForUDF.remove('tempBucketId')
        anonymizedDataDF = combinedDF.repartition('tempBucketId').withColumn("zipped_data", batchanonymizerengine_udf(struct(listOfColsForUDF))) \
                            .select(col('key').alias('originalCol'),col('zipped_data.zipped_data').alias('zipped_data'))

        # Explode the results to their own rows and split out the original and anonymized data to their own columns
        explodedDF = anonymizedDataDF.select("*",explode('zipped_data').alias('exploded')) \
                        .withColumn("id",col('exploded.id')) \
                        .withColumn("anonymizedData",col('exploded.anonymizedData')) \
                        .drop('zipped_data','exploded')

        # Pivot columns back into place based on the unique ID as grouping
        pivotedDF = explodedDF.groupBy("id").pivot("originalCol").agg(
            first("anonymizedData").alias("anonymizedData")
        )

        # Join back to analyzedDF without the columns with PII on them
        returnDF = analyzedDF.select('id',*[col for col in originalCols if col not in pivotedDF.columns]) \
                                    .join(pivotedDF,on='id') \
                                    .select([lit('<PII_MASKED>').alias(c) if c in knownPIICols else col(c) for c in originalCols]) \
                                    .select(originalCols) \
                                    .drop('id','tempBucketId','pii_analysis_result')

        return returnDF
        


    def anonymize_scanned_table_streaming(self, analyzedDF: DataFrame , parallelism_factor:int = 32) -> DataFrame: 
        """This version uses anonymizer_func (serial version) since it is challenging to use batchanonymizer_func in the context of streaming 
        due to that version having several groupBy and join transformations that does not work well in streaming, even with watermarks defined.
        
        Note: This version can also work with batch data as well as is the only current implementation that works with streaming data
        """

        analyzedCols = [field.name for field in analyzedDF.schema["pii_analysis_result"].dataType["analysis_results"].dataType.fields]
        knownPIICols = [field.name for field in analyzedDF.schema["pii_analysis_result"].dataType["known_pii_columns"].dataType.fields]
        originalCols = analyzedDF.columns.copy()

        anonymizerengine_udf = pandas_udf(PIIAnonymizer.anonymizer_func, returnType=StringType())

        anonymizedDF = analyzedDF.select('*')
        for analyzedCol in analyzedCols:
            anonymizedDF = anonymizedDF.withColumn(analyzedCol,
                                                    anonymizerengine_udf(
                                                                    struct([col(analyzedCol).alias('originalCol')
                                                                        ,col('pii_analysis_result.analysis_results.{}'.format(analyzedCol)).alias('recognizer_result')])))

        return anonymizedDF.repartition(parallelism_factor).select([lit('<PII_MASKED>').alias(c) if c in knownPIICols else col(c) for c in originalCols]).drop('pii_analysis_result')



    

