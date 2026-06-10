/**
 * Coarse geographic regions, matching the bounding boxes used by tx-simulator
 * to place cards. Used to detect when a card is used from a region it has
 * never transacted in before.
 */
public enum Region {
    POLAND,
    WESTERN_EUROPE,
    NORTH_AMERICA,
    EAST_ASIA,
    UNKNOWN;

    public static Region classify(GpsCoords loc) {
        double lat = loc.lat;
        double lon = loc.lon;

        if (lat >= 49.0 && lat <= 54.9 && lon >= 14.1 && lon <= 24.1) {
            return POLAND;
        }
        if (lat >= 43.0 && lat <= 53.0 && lon >= -5.0 && lon <= 15.0) {
            return WESTERN_EUROPE;
        }
        if (lat >= 25.0 && lat <= 50.0 && lon >= -125.0 && lon <= -65.0) {
            return NORTH_AMERICA;
        }
        if (lat >= 22.0 && lat <= 45.0 && lon >= 100.0 && lon <= 145.0) {
            return EAST_ASIA;
        }
        return UNKNOWN;
    }
}
