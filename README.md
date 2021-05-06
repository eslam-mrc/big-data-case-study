# big-data-case-study
Hello there!
This is, like the name suggests, a big data case study. This is an integration of Kafka, Spark, HDFS, Hive and Power BI. The main theme is to listen to what people are saying about a certain brand, trend or hashtag, do simple sentiment analysis on the tweets then reply accordingly and appropriately. After that, we save the data for further analysis.

### The pipeline goes as follows:
 - First, we gather data from twitter using the Twitter API.
 - Then we send the stream of data to Spark using Kafka.
 - After that we do some simple sentiment analysis on Spark, reply to the tweets and save parquet files to HDFS.
 - Finally, we store the data in a Hive table to be loaded by Microsoft Power BI for analysis and visualization.

### In this repository you will find:
 - The producer and consumer scripts.
 - A simple script to check the services using the Ambari-REST API.
 - The ODBC driver for connecting Hive with Power BI.
 - A walkthrough in the setup of this project.


Feel free to contact me if you have got any inquiries.
