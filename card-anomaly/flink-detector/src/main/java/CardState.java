import java.util.ArrayDeque;
import java.util.Deque;

public class CardState {
    public Transaction lastTransaction;
    public final Deque<Double> lastAmounts = new ArrayDeque<>();
    public final Deque<Long> recentTimestampsMillis = new ArrayDeque<>();

    public void addAmount(double amount) {
        lastAmounts.addLast(amount);

        while (lastAmounts.size() > 30) {
            lastAmounts.removeFirst();
        }
    }

    public double meanAmount() {
        if (lastAmounts.isEmpty()) {
            return 0.0;
        }

        double sum = 0.0;
        for (double value : lastAmounts) {
            sum += value;
        }

        return sum / lastAmounts.size();
    }

    public double stdAmount() {
        if (lastAmounts.size() < 2) {
            return 0.0;
        }

        double mean = meanAmount();
        double sumSquaredDiff = 0.0;

        for (double value : lastAmounts) {
            double diff = value - mean;
            sumSquaredDiff += diff * diff;
        }

        return Math.sqrt(sumSquaredDiff / lastAmounts.size());
    }

    public void addTimestamp(long timestampMillis) {
        recentTimestampsMillis.addLast(timestampMillis);

        long windowStart = timestampMillis - 60_000L;

        while (!recentTimestampsMillis.isEmpty()
                && recentTimestampsMillis.peekFirst() < windowStart) {
            recentTimestampsMillis.removeFirst();
        }
    }

    public int transactionsInLast60Seconds() {
        return recentTimestampsMillis.size();
    }
}