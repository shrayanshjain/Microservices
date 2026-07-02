package com.eda.streams.sourcetypes;

import com.eda.streams.model.Order;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * ═══════════════════════════════════════════════════════════════════════
 *  SOURCE TYPES DEMO — KStream vs KTable vs GlobalKTable
 * ═══════════════════════════════════════════════════════════════════════
 *
 *  EXAMPLE 1: KStream  — read "order-events" as a stream of events
 *  EXAMPLE 2: KTable   — read "customer-profiles" as a table (latest per key)
 *  EXAMPLE 3: GlobalKTable — read "countries" as a broadcast lookup table
 *
 *  All 3 read from different topics.
 *  Processor nodes are kept SIMPLE (peek, filter, toStream) — focus is on SOURCE.
 */
@Configuration
public class SourceTypesTopology {

    private static final ObjectMapper objectMapper = new ObjectMapper();


    //  EXAMPLE 1: KStream — every record is an independent event
    //@Bean
    public KStream<String, String> kstreamDemo(StreamsBuilder builder) {

        // SOURCE: read every record as an independent event
        KStream<String, String> orders = builder.stream("order-events",
                Consumed.with(Serdes.String(), Serdes.String()));

        // PROCESSOR: just peek (log each event as it flows through)
        orders.peek((key, value) ->
                System.out.println("[KStream] key=" + key + " value=" + value));

        // SINK: write to output topic
        orders.to("order-events-output",
                Produced.with(Serdes.String(), Serdes.String()));

        return orders;
    }


    //  EXAMPLE 2: KTable — latest value per key (like a DB table)
    // @Bean
    public KTable<String, String> ktableDemo(StreamsBuilder builder) {

        // SOURCE: read as table — maintains latest value per key in RocksDB
        KTable<String, String> orders  = builder.table("order-events",
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("order-events-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        // PROCESSOR: convert to stream so we can peek and write to output
        // (KTable has limited operations — no to(), no peek() directly)
        orders .toStream()
                .peek((key, value) ->
                        System.out.println("[KTable] key=" + key + " value=" + value))
                .to("order-events-output",
                        Produced.with(Serdes.String(), Serdes.String()));

        return orders;
    }


    //  EXAMPLE 3: GlobalKTable — full copy on every task/instance
    @Bean
    public  KStream<String, String> globalKTableDemo(StreamsBuilder builder) {

        GlobalKTable<String, String> countries = builder.globalTable("countries",
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("countries-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));


        KStream<String, String> ordersWithCountry = builder.stream("order-events",
                Consumed.with(Serdes.String(), Serdes.String()));


        KStream<String, String> enriched = ordersWithCountry.join(
                countries,
                (orderId, orderJson) -> {
                    try {
                        JsonNode order = objectMapper.readTree(orderJson);
                        return order.get("countryCode").asText();
                    } catch (Exception e) {
                        return "UNKNOWN";
                    }
                },
                (orderJson, countryName) ->
                        orderJson + " | country=" + countryName
        );

        enriched.peek((key, value) ->
                        System.out.println("[GlobalKTable JOIN] key=" + key + " value=" + value))
                .to("order-enriched-with-country",
                        Produced.with(Serdes.String(), Serdes.String()));

        return enriched;

    }

   // @Bean
    public void kStreamDemoWithJsonSerde(StreamsBuilder builder) {

        Serde<Order> orderSerde = new JsonSerde<>(Order.class);

        KStream<String, Order> orders = builder.stream("orders",
                                                    Consumed.with(Serdes.String(), orderSerde));

        orders.filter((key, order) -> order.getTotalAmount() > 100)
                .to("order-events-output",
                                 Produced.with(Serdes.String(), orderSerde));
    }
}

