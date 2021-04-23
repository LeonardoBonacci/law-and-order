package org.acme.kafka.lawandorder.streams;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import guru.bonacci.kafka.lawandorder.model.Node;
import guru.bonacci.kafka.lawandorder.model.NodeWrapper;
import guru.bonacci.kafka.serialization.JacksonSerde;

@ApplicationScoped
public class TopologyProducer {

	static final String STORE = "nodes-store";

    private static final String INPUT_TOPIC = "unordered-nodes";
    private static final String OUTPUT_TOPIC = "ordered-nodes";

	@SuppressWarnings("unchecked")
	@Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<Node> serde = JacksonSerde.of(Node.class);

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(STORE);

        StoreBuilder<KeyValueStore<String, Node>> storeBuilder = Stores.keyValueStoreBuilder(
        		storeSupplier,
                Serdes.String(),
                serde);

        builder.addStateStore(storeBuilder);

        Predicate<String, NodeWrapper> moveOnPredicate = (k, v) -> v.parentIsProcessed;
		Predicate<String, NodeWrapper> loopPredicate = (k, v) -> !moveOnPredicate.test(k, v);

		KStream<String, Node> unordered = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), serde));
		unordered.peek((k,v) -> System.out.println("incoming " + v.toString()));
		
		KStream<String, NodeWrapper>[] branched = unordered
							.transformValues(() -> new CrossRoadTransformer(STORE), STORE)
							.branch(moveOnPredicate, loopPredicate);
		// move on
		branched[0].mapValues(unwrap)
//		.peek((k,v) -> System.out.println("ordered " + v.toString()))
		.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), serde));

		// or back in line
        branched[1].mapValues(unwrap)
//		.peek((k,v) -> System.out.println("back " + v.toString()))
		.to(INPUT_TOPIC, Produced.with(Serdes.String(), serde));

        return builder.build();
    }
    
    ValueMapper<NodeWrapper, Node> unwrap = (wrapper) -> wrapper.node;
}