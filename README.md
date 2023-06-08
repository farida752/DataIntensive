# DataIntensive
# The system is composed of three stages
   #  Stage1: Data Acquisition -- Multiple weather stations(MicroServices) that feed a queueing service(Kafka)with their readings
   #  Stage2: Data Processing and Archiving -- The base central station is consuming the streamed data and archiving all data in the form of Parquet files
   # Stage3 Indexing  
         Two variants of index are maintained 
          Key-value store(Bitcask)for the latest reading from each individual station
          ElasticSearch/Kibana that are running over the Parquet files
#  Deployed using kubernetes
 
