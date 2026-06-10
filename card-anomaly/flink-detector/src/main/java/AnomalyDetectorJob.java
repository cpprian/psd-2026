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

            detectLargeAmount(tx, state, out);
            detectHighFrequency(tx, state, txTimeMillis, out);
            detectLimitExhaustion(tx, out);
            detectStructuring(tx, out);

            // A first visit to a new region always also looks like impossible
            // travel; report it only as NEW_GEOGRAPHY to avoid double-counting.
            boolean newRegion = detectNewGeography(tx, state, out);
            if (!newRegion) {
                detectImpossibleTravel(tx, state, txTimeMillis, out);
            }

            state.addAmount(tx.amount_pln);
            state.lastTransaction = tx;
        }

        private void detectLargeAmount(
                Transaction tx,
                CardState state,
                Collector<String> out
        ) throws Exception {
            // 5 transactions are enough history for a coarse baseline; waiting
            // for more delays the first possible alert per card considerably.
            if (state.lastAmounts.size() < 5) {
                return;
            }

            double mean = state.meanAmount();
            double std = state.stdAmount();

            if (std <= 0.0) {
                return;
            }

            double zScore = (tx.amount_pln - mean) / std;

            if (zScore >= 3.0 && tx.amount_pln >= 100.0) {
                String description = String.format(
                        "Amount %.2f PLN is %.1f standard deviations above recent mean %.2f PLN.",
                        tx.amount_pln,
                        zScore,
                        mean
                );

                // zScore == 3.0 (just over the trigger) -> 0.3, zScore >= 43 -> 1.0.
                double severity = Math.min(1.0, 0.3 + (zScore - 3.0) / 40.0);

                emitAlert(tx, AnomalyKind.LARGE_AMOUNT, description, severity, out);
            }
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

                // spentRatio == 0.95 (just over the trigger) -> 0.0, spentRatio == 1.0 -> 1.0.
                double severity = Math.min(1.0, Math.max(0.0, (spentRatio - 0.95) / 0.05));

                emitAlert(tx, AnomalyKind.LIMIT_EXHAUSTION, description, severity, out);
            }
        }

        private void detectHighFrequency(
                Transaction tx,
                CardState state,
                long txTimeMillis,
                Collector<String> out
        ) throws Exception {
            state.addTimestamp(txTimeMillis);

            int count = state.transactionsInLast60Seconds();

            if (count > 10) {
                String description = String.format(
                        "%d transactions detected for this card in the last 60 seconds.",
                        count
                );

                // count == 11 (just over the trigger) -> 0.1, count >= 20 -> 1.0.
                double severity = Math.min(1.0, (count - 10) / 10.0);

                emitAlert(tx, AnomalyKind.HIGH_FREQUENCY, description, severity, out);
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

            double distanceKm = previous.location.distanceKm(tx.location);

            if (deltaMillis < 60_000L) {
                return;
            }

            if (distanceKm < 300.0) {
                return;
            }

            double hours = deltaMillis / 3_600_000.0;
            double speedKmh = distanceKm / hours;

            if (speedKmh > 900.0) {
                String description = String.format(
                        "Card used %.0f km from previous location within %.1f minutes. Required travel speed: %.0f km/h.",
                        distanceKm,
                        deltaMillis / 60_000.0,
                        speedKmh
                );

                // Speed spans many orders of magnitude above the 900 km/h trigger,
                // so scale on a log axis: 900 km/h -> 0.0, 200 000 km/h -> 1.0.
                double severity = Math.min(1.0, Math.max(0.0,
                        (Math.log10(speedKmh) - Math.log10(900.0)) / (Math.log10(200_000.0) - Math.log10(900.0))
                ));

                emitAlert(tx, AnomalyKind.IMPOSSIBLE_TRAVEL, description, severity, out);
            }
        }

        /** Returns true when the transaction was reported as a new-geography alert. */
        private boolean detectNewGeography(Transaction tx, CardState state, Collector<String> out) throws Exception {
            Region region = Region.classify(tx.location);

            if (region == Region.UNKNOWN) {
                return false;
            }

            if (state.visitedRegions.isEmpty()) {
                // First transaction seen for this card - establish the baseline,
                // don't alert on it.
                state.visitedRegions.add(region);
                return false;
            }

            if (state.visitedRegions.contains(region)) {
                return false;
            }

            int visitedBefore = state.visitedRegions.size();
            state.visitedRegions.add(region);

            String description = String.format(
                    "Card used for the first time in a new region (%s). Previously seen in %d region(s).",
                    region, visitedBefore
            );

            // 1 region seen before -> 0.5, 3 regions seen before -> 0.9.
            double severity = Math.min(1.0, 0.3 + 0.2 * visitedBefore);

            emitAlert(tx, AnomalyKind.NEW_GEOGRAPHY, description, severity, out);
            return true;
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