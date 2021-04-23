package guru.bonacci.kafka.lawandorder.streams;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import guru.bonacci.kafka.lawandorder.model.NestedNode;
import guru.bonacci.kafka.lawandorder.model.Poor;
import guru.bonacci.kafka.lawandorder.model.PoorAndFlat;
import guru.bonacci.kafka.lawandorder.model.Rich;
import guru.bonacci.kafka.serialization.JacksonSerde;

public class ProductApp {

	public Properties buildProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Uuid.randomUuid().toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndFailExceptionHandler.class.getName());
        return props;
    }

    public Topology theTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

		Serde<NestedNode> nnSerde = JacksonSerde.of(NestedNode.class);
		Serde<Poor> pSerde = JacksonSerde.of(Poor.class);
		Serde<PoorAndFlat> pafSerde = JacksonSerde.of(PoorAndFlat.class);
		Serde<Rich> rSerde = JacksonSerde.of(Rich.class);
		
		KTable<String, NestedNode> nnTable = 
			builder.table("orderedT", Consumed.with(Serdes.String(), nnSerde));

		KTable<String, PoorAndFlat> pafTable = 
			builder.stream("poorT", Consumed.with(Serdes.String(), pSerde))
				.flatMapValues(Poor::toPaf)
				.toTable(Materialized.<String, PoorAndFlat, KeyValueStore<Bytes, byte[]>>as("paf")
	                        .withKeySerde(Serdes.String())
	                        .withValueSerde(pafSerde));

		pafTable.join( nnTable, 
					   PoorAndFlat::getFkId, 
					   (paf, n) -> Rich.builder().paf(paf).nn(n).build())
				.toStream()
				.filterNot((k,v) -> v == null)
				.to("richT", Produced.with(Serdes.String(), rSerde));

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        final ProductApp app = new ProductApp();
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