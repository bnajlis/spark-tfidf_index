import sys, getopt
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, DataFrameWriter
from pyspark.sql.functions import *

# ########################################################################
# TF-IDF Index builder funtion
##########################################################################
# Input Parameters:
#		sc: Spark Context (created in main function)
#		sqlContext: SparkSql Context (created in main function)
#		docs_path: string with path containing documents to index
# Returns:
#		tf_idf: DataFrame containing TF-IDF index
#			- Schema for DataFrame:
#				- doc_name: string containing original document name path
#				- word: word
#				- tf_idf: TF-IDF score for the word in doc_name
#########################################################################


def build_tf_idf_index(sc, sqlContext, docs_path, index_path):
	print("INFO Reading files from docs in " + docs_path + " ...")
	# Load files into RDD
	textFiles = sc.wholeTextFiles(docs_path)
	# Count number of documents in total to use in IDF calculations
	num_docs = textFiles.count() # ACTION

	print("INFO Building index for " + str(num_docs) + " files found...")
	###### Build TF-IDF index ######

	# Split document into words (still in RDD)
	tmpFilesRDD = textFiles.map(lambda docs:(docs[0], docs[1].split(" "))) #transformation
	# Convert RDD into Data Frame, to use SparkSQL
	textFilesDF = tmpFilesRDD.toDF(["doc_name", "doc_content"]) # ACTION
	# Explode the Data Frame to get one row per document*word combination. This DF is the basis for TF and IDF
	words_by_docDF = textFilesDF.select(textFilesDF.doc_name, explode(textFilesDF.doc_content).alias("word")) #transformation

	### Build Term Frequency table ###

	# First count each word in all documents
	pre_tf = words_by_docDF.groupBy(words_by_docDF.doc_name, words_by_docDF.word).count() #transformation
	# Do sum() aggregation of counts and set as column 'tf'
	tf = pre_tf.groupBy(words_by_docDF.doc_name, words_by_docDF.word, ).agg(sum("count").alias("tf")) #transformation

	### Build Inverse Document Frequency table ###

	# First do a count distinc group by words, to get the number of docs using each word
	pre_idf = words_by_docDF.distinct().groupby(words_by_docDF.word).count() #transformation
	# Now calculate IDF as log(num_docs / docs_using_word)
	idf = pre_idf.select(pre_idf.word, col("count"), log10(num_docs/(col("count"))).alias("idf")) #transformation

	# Join TF with IDF dataframes (TF left outer join IDF on word) so we have both TF and IDF per word*document
	pre_tf_idf = tf.join(idf, tf.word == idf.word, 'outer') #transformation
	
	# Build TF-IDF by multiplying TF * IDF
	tf_idf = pre_tf_idf.select(col("doc_name"), tf["word"], (col("tf") * col("idf")).alias("tf_idf")) #transformation
	print("Saving index to " + index_path + "spark-tfidf.parquet" + " ...")
	# Save index to HDFS parquet format
	try:
		tf_idf.write.save(index_path + "spark-tfidf.parquet")
	except:
		usage("Error saving index.")
	
	return tf_idf

def match_words(sc, tfidf_index, all_words, ndocs):
	
	print("INFO Finding matching documents...")
	# words are submittes as comma separated list, so we split the string into a python list first
	all_words = words.split(',')
	# count how many words are in the query for the normalization factor
	query_words_qty = len(all_words)
	# then we convert the list into a spark data frame (passing through a parallelized rdd and dummy lambda function)
	words_df = sc.parallelize(all_words).map(lambda x:(x,)).toDF(["query_word"])
	# get a subset data frame with just the query words
	joined = tfidf_index.join(words_df, words_df.query_word == tfidf_index.word, "inner")
	# counts how many matched words and aggregates (sum) the score per word
	pre_scored = joined.groupBy("doc_name").agg({"*":"count", "tf_idf":"sum"}).withColumnRenamed("count(1)", "matched_words_qty").withColumnRenamed("sum(tf_idf)", "pre_score")
	# calculate the tf_idf score for the document as pre_score (summ of per-word score) times number of matched words divided by num of words in query
	pre_scored2 = pre_scored.select(pre_scored.doc_name, (pre_scored.pre_score * pre_scored.matched_words_qty) / query_words_qty)
	# rename the matching score calculated column
	scored = pre_scored2.withColumnRenamed("((pre_score * matched_words_qty) / " + str(query_words_qty) + ")", "matching_score")
	# sort the documents by descending matching score and return only the top ndocs requested
	doc_matches = scored.sort("matching_score", ascending=False)
	if(int(ndocs) > 0):
		doc_matches = doc_matches.limit(int(ndocs))

	return doc_matches

def load_tf_idf_index(sc, sql, tf_idf_index_path):
	print("INFO Reading index from "+ tf_idf_index_path + "spark-tfidf.parquet")
	tf_idf_index = sql.read.parquet(tf_idf_index_path + "spark-tfidf.parquet")
	return tf_idf_index
	
def usage(err):
	# Prints error message and command line usage help
	print("ERROR " + err)
	print("Builds a TF IDF index from documents, saves it in parquet format, then searches for terms in the index and return the top X document matches.")
	print
	print("Usage: -w[words to search] -n[results] -d[docs to index path] -i[tfidf index path]")
	print
	print("Options:")
	print(" -d DOCUMENTS_TO_INDEX_PATH     hdfs:// or local file path with documents to index.")
	print("                                Optional if path in -i contains a tf-idf index.")
	print(" -i TF_IDF_INDEX_PATH           hdfs:// or local path to parquet tfidf index.")
	print("                                Path location has to end in '/' and contain no files.")
	print("                                If -d is specified will save resulting index here.")
	print("                                If -d is not specified, will try to load index from here.")
	print(" -w WORDS                       Comma-separated list of words to search")
	print(" -n RESULTS                     Optional: number of document results to return from index.")
	print("                                If not specified returns all results.")
	sys.exit()

# Main entry function
# Sets Spark and Sql Context to call Index builder and Document matching query 
def main(docs_path, words, ndocs, tfidf_index):
	c = SparkConf().setAppName("TF-IDF Indexer") # Set Spark configuration
	sc = SparkContext(conf = c)	# Set spark context with config
	sc.setLogLevel("ERROR")		# Reduce the error logging to minimum
	sql = SQLContext(sc)

	if docs_path:	# If a path to docs is specified...
		# ...build index from the docs...
		tf_idf_index = build_tf_idf_index(sc, sql, docs_path, tfidf_index)
	else:
		# ... or load a previously created and saved index ...
		tf_idf_index = load_tf_idf_index(sc, sql, tfidf_index)
		
	if words:
		matches = match_words(sc,tf_idf_index, words, ndocs)
		if(int(ndocs) > 0):
			matches.show(int(ndocs), False)
		else:
			matches.show(matches.count(), False)
	else:
		print("INFO No search terms specified, exiting...")

	sc.stop()					# Stop Spark context after done with main function


# Catch all to parse command line options and redirect to main function
if __name__ == "__main__":
	try:
		# Gets command line options required for script usage
		opts, args = getopt.getopt(sys.argv[1:],"d:w:n:i:")
	except getopt.GetoptError as err:
		usage(err);

	
	if(len(opts) == 0 ):
		usage("Error: Command line parameters not specified.")

	# Gets command line parameters
	tfidf_index = ""
	words = ""
	docs_path = ""
	ndocs = "-1"
	for opt,arg in opts:

		if opt == "-i":		
			tfidf_index = arg		# Path to tf_idf index
			if not tfidf_index:
				usage("Error: option -i missing.")
	

		if opt == "-w":
			words = arg				# List of words to use for query
			if not words:
				usage("Error: option -w missing.")

		if opt == "-d":
			docs_path = arg 		# Path containing documents to be indexed


		if opt == "-n":		
			ndocs = arg				# Number of documents to return in matching query
			

	
	main(docs_path, words, ndocs, tfidf_index)