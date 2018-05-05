import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;

public class flink_connector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = readFromKafka(env);

        writeToElasticsearch(stream);

        env.execute();
    }

    private static DataStream<String> readFromKafka(StreamExecutionEnvironment env) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink_consumer");

        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer09<>(
                "iot", new SimpleStringSchema(), properties) );
        return stream;
    }

    private static void writeToElasticsearch(DataStream<String> input) throws UnknownHostException {
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "my-application");
        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");
        config.put("node.name", "node-1");
        try {
            // Add elasticsearch hosts on startup
            List<InetSocketAddress> transports = new ArrayList<>();
            transports.add(new InetSocketAddress("localhost", 9300)); // port is 9300 not 9200 for ES TransportClient

            ElasticsearchSinkFunction<String> indexLog = new ElasticsearchSinkFunction<String>() {
                private IndexRequest createIndexRequest(String element) {
                    Map<String, String> esJson = new HashMap<>();
                    esJson.put("value", element);

                    return Requests
                            .indexRequest()
                            .index("testindex")
                            .type("weather")
                            .source(esJson);
                }

                @Override
                public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                    indexer.add(createIndexRequest(element));
                }
            };

            ElasticsearchSink esSink = new ElasticsearchSink(config, transports, indexLog);
            input.addSink(esSink);
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}

