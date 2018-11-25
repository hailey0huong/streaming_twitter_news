# Streaming Tweets and Analyzing News
Apply big data technologies to build a data pipeline for twitter streaming data and merge with News data for analytics

## Tweet Streaming
There are 2 experiments conducted with Twitter Streaming data. The high-level workflows are illustrated below. 
![](twitter_data_workflow.jpg)

For both processes, streaming tweets are listened by Kafka. Code to connect Twitter API and Kafka can be found at **twitter_to_kafka.py**.


In the first process, data are cleaned in real-time for storage using Spark Streaming framework and Python. Code for this process can be found at **tweetcollect_kafka.py**.

In the second process, data are cleaned and analyze real-time using Spark Streaming framework and Python. The results are updated in real-time in my Plotly Dashboard as below. Code for this part is at **StreamSensing_wVizz.py**.

![](output_FvhBEC.gif)

## News Analytics

