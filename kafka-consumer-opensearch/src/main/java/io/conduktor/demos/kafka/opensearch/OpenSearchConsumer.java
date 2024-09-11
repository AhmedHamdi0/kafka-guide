package io.conduktor.demos.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {
	private static RestHighLevelClient createOpenSearchClient() {
		RestHighLevelClient restHighLevelClient;
		URI connUri = URI.create("http://localhost:9200");
		String userInfo = connUri.getUserInfo();
		
		if (userInfo == null)  {
			restHighLevelClient =  new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme())));
		} else {
			String[] auth = userInfo.split(":");
			CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));
			
			restHighLevelClient =  new RestHighLevelClient(
				RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
					.setHttpClientConfigCallback(
						httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
							.setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
					)
			);
			
		}
		return restHighLevelClient;
	}
	
	private static KafkaConsumer<String, String> createKafkaConsumer() {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-opensearch-demo");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		
		return new KafkaConsumer<>(properties);
	}
	
	private static String extractId(String json_record) {
		return JsonParser.parseString(json_record)
			.getAsJsonObject()
			.get("meta")
			.getAsJsonObject()
			.get("id")
			.getAsString();
	}

	public static void main(String[] args) throws IOException {
		Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
		RestHighLevelClient openSearchClient = createOpenSearchClient();
		KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();
		kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"));
		
		try(openSearchClient; kafkaConsumer) {
			Boolean wikimediaIndexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
			
			if (!wikimediaIndexExists) {
				CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
				openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
				log.info("The Wikimedia Index is created!");
			} else {
				log.info("The Wikimedia Index already exists!!");
			}
			
			while (true) {
				ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));
				BulkRequest bulkRequest = new BulkRequest();
				
				for (ConsumerRecord<String, String> record : records) {
					try {
						String record_id = extractId(record.value());
						IndexRequest indexRequest = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON).id(record_id);
						bulkRequest.add(indexRequest);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				
				if (bulkRequest.numberOfActions() > 0) {
					BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
					log.info("Inserted " + bulkResponse.getItems().length + " record(s).");
					
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}
