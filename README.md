# Crypto-Data-Pipeline-Using-GCP

![Data_pipeline](https://github.com/ravidu-rupasinghe/Crypto-Data-Pipeline-Using-GCP/blob/main/pipeline1.gif)

This pipeline demonstrates a comprehensive end-to-end solution for ingesting, processing, and analyzing cryptocurrency data from the CoinGecko API using Google Cloud Platform (GCP). The pipeline begins with raw data collection into Cloud Storage, orchestrated by Airflow running on Cloud Composer. The data is then transformed and stored in Cloud Storage before being loaded into BigQuery for SQL querying. Finally, Looker is used to visualize the results of these queries, turning raw cryptocurrency data into actionable insights.
