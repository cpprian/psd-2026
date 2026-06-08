import com.fasterxml.jackson.annotation.JsonProperty;

public enum AnomalyKind {
    @JsonProperty("large_amount")
    LARGE_AMOUNT("large_amount"),

    @JsonProperty("impossible_travel")
    IMPOSSIBLE_TRAVEL("impossible_travel"),

    @JsonProperty("high_frequency")
    HIGH_FREQUENCY("high_frequency"),

    @JsonProperty("new_geography")
    NEW_GEOGRAPHY("new_geography"),

    @JsonProperty("limit_exhaustion")
    LIMIT_EXHAUSTION("limit_exhaustion"),

    @JsonProperty("structuring")
    STRUCTURING("structuring");

    private final String jsonName;

    AnomalyKind(String jsonName) {
        this.jsonName = jsonName;
    }

    @Override
    public String toString() {
        return jsonName;
    }
}