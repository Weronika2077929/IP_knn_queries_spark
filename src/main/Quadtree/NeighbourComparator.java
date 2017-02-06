import java.util.Comparator;

/**
 * Created by Wera on 06/02/2017.
 */
public class NeighbourComparator implements Comparator<Neighbour> {

    @Override
    public int compare(Neighbour n1, Neighbour n2) {
        if(n1.getDistance() < n2.getDistance())
            return -1;
        return 1;
    }
}
