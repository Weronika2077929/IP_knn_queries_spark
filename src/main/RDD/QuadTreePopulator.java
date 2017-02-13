import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


/**
 * Created by Wera on 29/01/2017.
 */
public class QuadTreePopulator {

    private static String FILE_PATH = "C:/Users/Wera/Documents/4thyear/IP/Java_Spark_Project/src/main/resources/";
    private static String FILE_NAME_DATASET = FILE_PATH + "1000";
    private static String FILE_NAME_QUERY_POINTS = FILE_PATH + "10";
    private static double x_coordinate = 0;
    private static double y_coordinate = 0;
    private static int k = 5;

    public static void main( String[] args ){

        try {
            FileUtils.cleanDirectory(new File(FILE_PATH + "quadtree_data/"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        QuadTreeArray quadTree = new QuadTreeArray(0,0,1000000,1000000);

        try {
            Scanner in = new Scanner(new FileReader(FILE_NAME_DATASET));
            while(in.hasNext()) {
                String[] data = in.nextLine().split(",");
                quadTree.set(Double.parseDouble(data[0]), Double.parseDouble(data[1]), null);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        LinkedList<Point> queryPoints = new LinkedList<>();
//        Skips first line in the file
        try {
            Scanner in = new Scanner(new FileReader(FILE_NAME_QUERY_POINTS));
            while(in.hasNext()) {
                String[] data = in.nextLine().split(",");
                queryPoints.add(new Point(Double.parseDouble(data[0]), Double.parseDouble(data[1]), null));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        for (Point queryPoint : queryPoints){
            System.out.println(queryPoint.getX() + " " + queryPoint.getY());
            nnQuery(quadTree, queryPoint.getX(), queryPoint.getY());
            System.out.println();
            System.out.println();
        }

    }

    public static void nnQuery(QuadTreeArray quadTree, double x, double y) {
        NodeArray mainPartition = quadTree.findPariton(x, y);
        List<Neighbour> nearestNeighbours = findNearestNeighbours(mainPartition, x ,y);

        Circle circle = findFurthestNeighbourCircle( x, y, nearestNeighbours);

//         one day change it to array list because get is an expensive fn in LinkedList
        LinkedList<NodeArray> partitons = quadTree.findPartitions(quadTree.getRootNodeArray(), circle, mainPartition);

        for ( NodeArray partition : partitons){
            nearestNeighbours.addAll(findNearestNeighbours(partition, x ,y));
        }

        Collections.sort(nearestNeighbours, new NeighbourComparator());

        for( int i = 0; i < k ; i++){
            System.out.println(i+1 + ") " + nearestNeighbours.get(i).toString());
        }
    }

    private static Circle findFurthestNeighbourCircle(double x_coordinate, double y_coordinate, List<Neighbour> nearestNeighbours) {
        Neighbour furthestNeighbour = nearestNeighbours.get(k-1);
        double radious = Math.sqrt(Math.pow(x_coordinate - furthestNeighbour.getPoint().getX(), 2) + Math.pow(y_coordinate - furthestNeighbour.getPoint().getY(), 2));
        System.out.println("Radious: " + radious);
        System.out.println("Furthest point: " + furthestNeighbour.getPoint().getX() + " " + furthestNeighbour.getPoint().getY() + " " + furthestNeighbour.getDistance());
        return new Circle(x_coordinate, y_coordinate, radious);
    }

    private static List<Neighbour> findNearestNeighbours(NodeArray partition, double x_coordinate, double y_coordinate) {

        LinkedList<Neighbour> nearestNeighbours = new LinkedList<>();

        for (Point point : partition.getPoints()) {
            double distance = Math.sqrt(Math.pow(x_coordinate - point.getX(), 2) + Math.pow(y_coordinate - point.getY(), 2));
            nearestNeighbours.add(new Neighbour(point,distance));
        }
//      not sure it sort is needed here
        Collections.sort(nearestNeighbours, new NeighbourComparator());

        return nearestNeighbours;
    }
}


