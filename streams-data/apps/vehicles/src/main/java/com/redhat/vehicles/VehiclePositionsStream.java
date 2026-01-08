package com.redhat.vehicles;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;

@ApplicationScoped
public class VehiclePositionsStream {

    // Deserializer for NULL keys.
    private final Serde<String> stringSerde = Serdes.String();

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        ObjectMapperSerde<VehiclePosition> vehiclePositionSerde = new ObjectMapperSerde<>(
            VehiclePosition.class
        );

        var first = builder.stream("vehicle-positions", Consumed.with(stringSerde, vehiclePositionSerde));
        first.peek((key, value) -> System.out.printf("vehicle position: %s=%s\n", key, value))
            .map((key, value) -> {
                var elevationInFeet = value.elevation * 3.28084;
                return KeyValue.pair(value.vehicleId, elevationInFeet);
            })
            .to(
                "vehicle-feet-elevations",
                Produced.with(Serdes.Integer(), Serdes.Double())
            );

        var second = builder.stream("vehicle-positions", Consumed.with(stringSerde, vehiclePositionSerde));
        second.groupBy((key, value) -> value.vehicleId, Grouped.with(Serdes.Integer(), vehiclePositionSerde))
            .count()
            .toStream()
            .foreach((vehicleId, count) -> System.out.printf("Vehicle: %s Positions count %s\n\n", vehicleId, count));

        return builder.build();
    }
}
