
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from textblob import TextBlob

def findSubjectivity(twt):
    return TextBlob(twt).sentiment.subjectivity

def findPolarity(twt):
    return TextBlob(twt).sentiment.polarity

def findAnalysis(score):
    if score < 0:
        return 'Negative'
    elif score == 0:
        return 'Neutral'
    else:
        return 'Positive'

def findSentiment(expr):
    polarityUdf = udf(findPolarity, StringType())
    expr = expr.withColumn("polarity", polarityUdf("tweet"))
    subjectivityUdf = udf(findSubjectivity, StringType())
    expr = expr.withColumn("subjectivity", subjectivityUdf("tweet"))
    analysisUdf = udf(findAnalysis, StringType())
    expr = expr.withColumn("analysis", analysisUdf(col('polarity')))
    return expr

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()
    tweets = spark.readStream.format("socket").option("host", "localhost").option("port", 12346).load()

    expr = tweets.select(explode(split(tweets.value, "t_end")).alias("tweet"))
    expr = expr.na.replace('', None)
    expr = expr.na.drop()
    expr = expr.withColumn('tweet', F.regexp_replace('tweet', "(RT)|:|#|(@\w+)|('http\S+')", ''))
    expr = findSentiment(expr)

    ssc = expr.writeStream.outputMode("update").format("console").start()
    ssc.awaitTermination()
