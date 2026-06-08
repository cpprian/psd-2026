import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class AnomalyDetectorJob {

    private static final String DEFAULT_BROKER = "kafka:29092";
    private static final String TOPIC_TRANSACTIONS = "transactions";
    private static final String TOPIC_ALERTS = "alerts";

    public static void main(String[] args) throws Exception {
        String broker = args.length > 0 ? args[0] : DEFAULT_BROKER;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(broker)
                .setTopics(TOPIC_TRANSACTIONS)
                .setGroupId("flink-detector")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(broker)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(TOPIC_ALERTS)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka transactions source")
                .flatMap(new DetectorFunction())
                .sinkTo(sink);

        env.execute("Card anomaly detector");
    }

    public static class DetectorFunction extends RichFlatMapFunction<String, String> {

        private transient ObjectMapper mapper;
        private transient Map<String, CardState> cardStates;

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper();
            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

            cardStates = new HashMap<>();
        }

        @Override
        public void flatMap(String json, Collector<String> out) throws Exception {
            Transaction tx;

            try {
                tx = mapper.readValue(json, Transaction.class);
            } catch (Exception e) {
                System.err.println("Could not parse transaction JSON: " + json);
                return;
            }

            if (tx.card_id == null || tx.timestamp == null || tx.location == null) {
                return;
            }

            CardState state = cardStates.computeIfAbsent(tx.card_id, ignored -> new CardState());

            long txTimeMillis;

            try {
                txTimeMillis = OffsetDateTime.parse(tx.timestamp).toInstant().toEpochMilli();
            } catch (Exception e) {
                System.err.println("Could not parse timestamp: " + tx.timestamp);
                return;
            }

            detectLimitExhaustion(tx, out);
            detectStructuring(tx, out);
            detectImpossibleTravel(tx, state, txTimeMillis, out);

            state.addAmount(tx.amount_pln);
            state.addTimestamp(txTimeMillis);
            state.lastTransaction = tx;
        }

        private void detectLimitExhaustion(Transaction tx, Collector<String> out) throws Exception {
            double totalBeforeTransaction = tx.amount_pln + tx.remaining_limit_pln;

            if (totalBeforeTransaction <= 0.0) {
                return;
            }

            double spentRatio = tx.amount_pln / totalBeforeTransaction;

            if (spentRatio >= 0.95 && tx.amount_pln >= 100.0) {
                String description = String.format(
                        "Single transaction spent %.1f%% of available card limit. Amount: %.2f PLN, remaining limit: %.2f PLN.",
                        spentRatio * 100.0,
                        tx.amount_pln,
                        tx.remaining_limit_pln
                );

                double severity = Math.min(1.0, spentRatio);

                emitAlert(tx, AnomalyKind.LIMIT_EXHAUSTION, description, severity, out);
            }
        }

        private void detectStructuring(Transaction tx, Collector<String> out) throws Exception {
            double[] thresholds = {500.0, 1000.0, 5000.0};

            for (double threshold : thresholds) {
                double diff = threshold - tx.amount_pln;

                if (diff > 0.0 && diff <= 50.0) {
                    String description = String.format(
                            "Transaction amount %.2f PLN is just below %.0f PLN threshold.",
                            tx.amount_pln,
                            threshold
                    );

                    double severity = Math.min(1.0, 1.0 - diff / 50.0);

                    emitAlert(tx, AnomalyKind.STRUCTURING, description, severity, out);
                    return;
                }
            }
        }

        private void detectImpossibleTravel(
                Transaction tx,
                CardState state,
                long txTimeMillis,
                Collector<String> out
        ) throws Exception {
            Transaction previous = state.lastTransaction;

            if (previous == null || previous.location == null || previous.timestamp == null) {
                return;
            }

            long previousTimeMillis;

            try {
                previousTimeMillis = OffsetDateTime.parse(previous.timestamp).toInstant().toEpochMilli();
            } catch (Exception e) {
                return;
            }

            long deltaMillis = txTimeMillis - previousTimeMillis;

            if (deltaMillis <= 0) {
                return;
            }

            double hours = deltaMillis / 3_600_000.0;
            double distanceKm = previous.location.distanceKm(tx.location);
            double speedKmh = distanceKm / hours;

            if (speedKmh > 900.0) {
                String description = String.format(
                        "Card used %.0f km from previous location within %.1f minutes. Required travel speed: %.0f km/h.",
                        distanceKm,
                        deltaMillis / 60_000.0,
                        speedKmh
                );

                double severity = Math.min(1.0, speedKmh / 5000.0);

                emitAlert(tx, AnomalyKind.IMPOSSIBLE_TRAVEL, description, severity, out);
            }
        }

        private void emitAlert(
                Transaction tx,
                AnomalyKind kind,
                String description,
                double severity,
                Collector<String> out
        ) throws Exception {
            Alert alert = new Alert(
                    UUID.randomUUID().toString(),
                    tx.transaction_id,
                    tx.card_id,
                    tx.user_id,
                    Instant.now().toString(),
                    kind.toString(),
                    description,
                    severity,
                    tx
            );

            String alertJson = mapper.writeValueAsString(alert);
            out.collect(alertJson);
        }
    }
}