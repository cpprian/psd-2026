import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction {
    public String transaction_id;
    public String card_id;
    public String user_id;
    public String timestamp;
    public GpsCoords location;
    public double amount_pln;
    public double remaining_limit_pln;
    public String merchant;

    @JsonProperty("injected_anomaly")
    public String injected_anomaly;

    public Transaction() {
    }
}