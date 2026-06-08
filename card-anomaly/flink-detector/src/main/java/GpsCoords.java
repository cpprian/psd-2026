public class GpsCoords {
    public double lat;
    public double lon;

    public GpsCoords() {
    }

    public GpsCoords(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
    }

    public double distanceKm(GpsCoords other) {
        final double R = 6371.0;

        double dLat = Math.toRadians(other.lat - this.lat);
        double dLon = Math.toRadians(other.lon - this.lon);

        double a =
                Math.pow(Math.sin(dLat / 2.0), 2)
                        + Math.cos(Math.toRadians(this.lat))
                        * Math.cos(Math.toRadians(other.lat))
                        * Math.pow(Math.sin(dLon / 2.0), 2);

        return 2.0 * R * Math.asin(Math.sqrt(a));
    }
}