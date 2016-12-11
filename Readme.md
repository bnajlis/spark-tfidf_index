# TF-IDF Index implemented in PySpark

As part of the final exam assignment for my Masters in Data Science course “DS8003 – Management of Big Data Tools”, I created a Big Data TF-IDF index builder and query tool. The tool consists a script with functions to create a TF-IDF (term frequency-inverse document frequency) index and it is then used it to return matching queries for a list of terms provided and number of results expected.

## Features Summary

*	Developed with PySpark, SparkSQL and DataFrames API for maximum compatibility with Spark 2.0
*	Documents to build the TF-IDF index can be on a local or HDFS path
*	Index is stored in parquet format in HDFS
*	Query terms and number of results are specified via command line arguments

The choice to use a combination of Spark, SparkSQL and the DataFrames API over all other possible tools that we covered in class and I could have used (including MapReduce, Hive, Pig, HBase, MongoDB) is purely personal: I believe that Spark is the best Big Data platform in terms of performance and long term, and through the course is the one that proved the easiest to work with and better performance dealing with large data sets. 
The Data frame schema is as follows, please see the sample data set below:

| doc_name                    | word    | tf_idf |
|-----------------------------|---------|--------|
| hdfs://sandbox.hortonworks… | spark   | 0.6999 |
| hdfs://sandbox.hortonworks… | is      | 0.6999 |
| hdfs://sandbox.hortonworks… | awesome | 0.6999 |

Using the parquet format for the tf-idf index store is another design choice based on my personal experience with columnar format stores on other platforms. Considering the schema is a denormalized list of documents-words, the expected cardinality of these two columns makes them very good candidates for the high compression rates that columnar stores have.

The code is a python script to be used with spark-submit as a submit job, but it can easily be adapted to other uses.

## Program flow Description
The following diagram shows the three main functions used in the script with their input and output parameters.

### build_tf_idf_index (line 22)

This is the main function that builds the tf-idf index, and takes four parameters:
*	sparkContext: required to load the document files to be indexed into an RDD
*	sqlContext: required to convert the RDD into DataFrame and apply transformations to it
*	docsInPath: string containing the HDFS path where the documents to index are stored
*	indexOutPath: string containing the HDFS path where the parquet tf-idf index will be stored

### load_tf_idf_index (line 96)

This function loads a previously saved tf-idf index and outputs a data frame containing the index. It only has two input parameters:
*	sqlContext: to load the parquet data frame from HDFS
*	indexInPath: string containing the HDFS path where the tf-idf index was stored
and just one output parameter:
*	tf_idf_index: data frame containing the index

### match_words (line 68)

This function looks for word matches in an index. It takes four parameters:
*	sparkContext: to apply transformations
*	indexDataFrame: data frame containing the tf-idf-index
*	words: list of words to search the index for
*	numDocsToDisplay: number of results to show
and it prints a table with the results in the console.

## How to use the tf-idf-indexer.py script

The index can be invoked using spark-submit with the following options:

Usage: -w[words to search] -n[results] -d[docs to index path] -i[tfidf index path]
Options:

-d DOCUMENTS_TO_INDEX_PATH     hdfs:// or local file path with documents to index.
                                                                  Optional if path in -i contains a tf-idf index.
-i TF_IDF_INDEX_PATH           hdfs:// or local path to parquet tfidf index.
                                                    Path location has to end in '/' and contain no files.
                                                    If -d is specified will save resulting index here.
                                                    If -d is not specified, will try to load index from here.
-w WORDS                       Comma-separated list of words to search
-n RESULTS                     Optional: number of document results to return from index.
                                         If not specified returns all results.


