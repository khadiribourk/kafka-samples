package com.github.kibourk.kafka;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {
    final static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
    public static final String GROUP_ID = "my-elasticsearch-app";
    public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public static final String AUTO_OFFSET_RESET_CONFIG = "earliest";

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();

        /*String jsonString = "{\"foo\":\"bar\"}";
        IndexRequest indexRequest = new IndexRequest(
                "twitter", "tweets").source(jsonString, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        logger.info(id);*/
        String isManualCommitOfOffsets="false";
        KafkaConsumer<String, String> consumer =createConsumer("twitter_tweets",isManualCommitOfOffsets);
        //poll for the new data
        while(true){
            ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));
            logger.info("Received "+records.count()+" records");
            for(ConsumerRecord record:records){
                //2 strategies
                //1- kafka generic ID
                //---->String id=record.topic()+record.partition()+record.offset();
                //2- twitter feed specific id
                String id=extractIdFromTweet(record.value().toString());
                //Insert data into Elasticsearch
                //logger.info(record.value().toString());
                IndexRequest indexRequest = new IndexRequest(
                        "twitter",
                        "tweets",
                        id) //this is to ùake our consumer idempotent
                        .source(record.value().toString().replace("."," "), XContentType.JSON);
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

                logger.info(indexResponse.getId());

                //for the demo purpose
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            //Testing manual commit of offsets [10 records per poll request]
            if(isManualCommitOfOffsets.equals("false")){
               logger.info("Commiting offsets");
               consumer.commitSync();
               logger.info("Ofssets have been committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //client.close();
    }

    private static JsonParser  jsonParser=new JsonParser();
    private static String extractIdFromTweet(String tweetJson) {
        //gson library
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static RestHighLevelClient createClient() {
        //https://VsnGtv5Hfc:FpnTMNvtWJhUmC2y5rkf3sc@kafka-course-2541909536.eu-west-1.bonsaisearch.net
        String username = "VsnGtv5Hfc";
        String hostname = "kafka-course-2541909536.eu-west-1.bonsaisearch.net";
        String password = "FpnTMNvtWJhUmC2y5rkf3sc";

        //don't do if you run a local ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        return new RestHighLevelClient(builder);
    }

    public static KafkaConsumer<String, String> createConsumer(String topic,String isManualCommitOfOffsets ) {
        //create the consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_CONFIG);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,isManualCommitOfOffsets);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10");//10 records per poll request

        //Create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe to our topic
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
}
