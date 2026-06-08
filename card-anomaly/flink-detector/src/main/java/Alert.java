public class Alert {
    public String alert_id;
    public String transaction_id;
    public String card_id;
    public String user_id;
    public String timestamp;
    public String anomaly_kind;
    public String description;
    public double severity;
    public Transaction transaction;

    public Alert() {
    }

    public Alert(
            String alert_id,
            String transaction_id,
            String card_id,
            String user_id,
            String timestamp,
            String anomaly_kind,
            String description,
            double severity,
            Transaction transaction
    ) {
        this.alert_id = alert_id;
        this.transaction_id = transaction_id;
        this.card_id = card_id;
        this.user_id = user_id;
        this.timestamp = timestamp;
        this.anomaly_kind = anomaly_kind;
        this.description = description;
        this.severity = severity;
        this.transaction = transaction;
    }
}