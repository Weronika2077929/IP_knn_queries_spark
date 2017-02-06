/**
 * Created by Wera on 06/02/2017.
 */
public class Neighbour {
    private Point point;
    private double distance;

    public Neighbour(Point point, double distance) {
        this.point = point;
        this.distance = distance;
    }

    public Point getPoint() {
        return point;
    }

    public double getDistance() {
        return distance;
    }
}
