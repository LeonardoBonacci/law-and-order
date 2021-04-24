package guru.bonacci.kafka.lawandorder.domain;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import guru.bonacci.kafka.lawandorder.model.Enlightened;
import guru.bonacci.kafka.lawandorder.model.Ignorant;
import guru.bonacci.kafka.lawandorder.model.IgnorantFlat;
import guru.bonacci.kafka.lawandorder.model.NestedNode;
import guru.bonacci.kafka.lawandorder.model.Node;
import guru.bonacci.kafka.serialization.JacksonSerde;

public class Schemas {

	public static String schemaRegistryUrl = "";
	public static Serde<IgnorantFlat> IGNORANT_FLAT_VALUE_SERDE;
	
	public static class Topic<K, V> {

		private final String name;
		private final Serde<K> keySerde;
		private final Serde<V> valueSerde;

		Topic(final String name, final Serde<K> keySerde, final Serde<V> valueSerde) {
			this.name = name;
			this.keySerde = keySerde;
			this.valueSerde = valueSerde;
			Topics.ALL.put(name, this);
		}

		public Serde<K> keySerde() {
			return keySerde;
		}

		public Serde<V> valueSerde() {
			return valueSerde;
		}

		public String name() {
			return name;
		}

		public String toString() {
			return name;
		}
	}

	public static class Topics {

		public final static Map<String, Topic> ALL = new HashMap<>();
		public static Topic<String, Enlightened> ENLIGHTENED;
		public static Topic<String, Ignorant> IGNORANT;
		public static Topic<String, String> DIGEST;
		public static Topic<String, Node> UNORDERED;
		public static Topic<String, NestedNode> ORDERED;

		static {
			createTopics();
		}

		private static void createTopics() {
			// having or showing a rational, modern, and well-informed outlook.
			ENLIGHTENED = new Topic<>("enlightened", Serdes.String(), JacksonSerde.of(Enlightened.class));
			// lacking knowledge or awareness in general; uneducated or unsophisticated.
			IGNORANT = new Topic<>("ignorant", Serdes.String(), JacksonSerde.of(Ignorant.class));
			IGNORANT_FLAT_VALUE_SERDE = JacksonSerde.of(IgnorantFlat.class);
			
			DIGEST = new Topic<>("digested", Serdes.String(), Serdes.String());
			UNORDERED = new Topic<>("unordered", Serdes.String(), JacksonSerde.of(Node.class));
			ORDERED = new Topic<>("ordered", Serdes.String(), JacksonSerde.of(NestedNode.class));
		}
	}

	public static void configureSerdesWithSchemaRegistryUrl(final String url) {
		Topics.createTopics();
		for (final Topic topic : Topics.ALL.values()) {
			configure(topic.keySerde(), url);
			configure(topic.valueSerde(), url);
		}
		schemaRegistryUrl = url;
	}

	private static void configure(final Serde serde, final String url) {
//		if (serde instanceof SpecificAvroSerde) {
//			serde.configure(Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, url), false);
//		}
	}
}
