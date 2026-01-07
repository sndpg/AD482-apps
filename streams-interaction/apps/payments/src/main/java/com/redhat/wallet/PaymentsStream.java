package com.redhat.wallet;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

@ApplicationScoped
public class PaymentsStream {

    // Deserializer for message keys.
    private final Serde<String> keySerde = Serdes.String();

    // Serializer for message values
    private final Serde<Integer> valueSerde = Serdes.Integer();

    @Produces
    public Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder
            .stream("payments", Consumed.with(keySerde, valueSerde))
            .peek((key, value) -> System.out.println(key + ": " + value))
            .filter((key, value) -> value >= 1000)
            .to("large-payments");
        return streamsBuilder.build();
    }
}
