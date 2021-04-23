package guru.bonacci.kafka.lawandorder.streams;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import guru.bonacci.kafka.lawandorder.model.NestedNode;
import guru.bonacci.kafka.lawandorder.model.Node;
import guru.bonacci.kafka.lawandorder.model.NodeWrapper;
import guru.bonacci.kafka.serialization.JacksonSerde;

public class HierarchyApp {

	public Properties buildProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Uuid.randomUuid().toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndFailExceptionHandler.class.getName());
        return props;
    }

	public Topology theTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<Node> nodeSerde = JacksonSerde.of(Node.class);
        Serde<NestedNode> nnodeSerde = JacksonSerde.of(NestedNode.class);

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore("store");

        StoreBuilder<KeyValueStore<String, NestedNode>> storeBuilder = Stores.keyValueStoreBuilder(
        		storeSupplier,
                Serdes.String(),
                nnodeSerde);

        builder.addStateStore(storeBuilder);

        Predicate<String, NodeWrapper> moveOnPredicate = (k, v) -> v.parentIsProcessed;
		Predicate<String, NodeWrapper> loopPredicate = (k, v) -> !moveOnPredicate.test(k, v);

		KStream<String, Node> unordered = 
			builder.stream("unorderedT", Consumed.with(Serdes.String(), nodeSerde));
		
		Consumer<KStream<String, NodeWrapper>> forward = moveOn -> 
			moveOn.mapValues((wrap) -> wrap.pnode)
				  .to("orderedT", Produced.with(Serdes.String(), nnodeSerde));

		Consumer<KStream<String, NodeWrapper>> back = loopBack -> 
			loopBack.mapValues((wrap) -> wrap.pnode.toNode())
				  .to("unorderedT", Produced.with(Serdes.String(), nodeSerde));
		
		unordered
			.transformValues(() -> new CrossRoadTransformer(), "store")
			.split()
			.branch(moveOnPredicate, Branched.withConsumer(forward))
			.branch(loopPredicate, Branched.withConsumer(back));

		return builder.build();
    }

    public static void main(String[] args) throws Exception {
        final HierarchyApp app = new HierarchyApp();
        final Topology topology = app.theTopology();
        final KafkaStreams streams = new KafkaStreams(topology, app.buildProperties());
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(5));
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}