# Recent Trends in ML, AI, and Bigdata news
Goal: tracking trending topics in big data, machine learning, and AI.

There are 2 sources of news to analyze: news sharing through tweets and news from traditional news media. 

## Twitter Streaming
There are 2 experiments conducted with Twitter Streaming data. The high-level workflows are illustrated below. 
![](twitter_data_workflow.jpg)

For both experiments, streaming tweets are listened by Kafka. Code to connect Twitter API and Kafka can be found at **twitter_to_kafka.py**.
To run the code in your terminal window, type `python2.7 twitter_to_kafka.py`

In the first experiment, data is collected from Kafka streams, cleaned in real-time, and sent to Cosmos DB for storage using Spark Streaming framework and Python. Code for this process can be found at **tweetcollect_kafka.py**.
To run the whole process, type `spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 tweetcollect_kafka.py`

In the second experiment, data is cleaned and analyzed in real-time using Spark Streaming framework and Python. The results are updated in seconds in a Plotly Dashboard as below. Code for this part is at **StreamSensing_wVizz.py**.
To run the whole process, type `spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 StreamSensing_wVizz.py`

![](output_FvhBEC.gif)

## Articles Collecting
The workflow for collecting and analyzing articles from traditional news media is below.

![](newsapi_flow.png)

## News Analytics
Codes to query data from Cosmos DB and prepare dataframes for analysis in Spark can be found at **connect_cosmos_spark.py**

Data retrieved from Cosmos DB are cleaned up further on Spark for the purpose of analysis. Codes can be found at **clean_data_on_spark.py**

LDA analysis was conducted on News corpus at **LDA_news.py**

Full analysis and visualization on Twitter and News data can be found at **twitter_news_analytics.py**

