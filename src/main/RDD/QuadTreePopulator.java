import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.Collator;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Wera on 29/01/2017.
 */
public class QuadTreePopulator {

    private static String FILE_PATH = "C:/Users/Wera/Documents/4thyear/IP/Java_Spark_Project/src/main/resources/";
    private static String FILE_NAME = FILE_PATH + "1";
    private static long x_coordinate = 0;
    private static long y_coordinate = 0;
    private static int k = 5;

    public static void main( String[] args ){

        try {
            FileUtils.cleanDirectory(new File(FILE_PATH + "quadtree_data/"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        QuadTreeArray quadTree = new QuadTreeArray(0,0,10,10);

        try {
            Scanner in = new Scanner(new FileReader(FILE_NAME));
            while(in.hasNext()) {
                String[] data = in.nextLine().split(",");
                quadTree.set(Double.parseDouble(data[0]), Double.parseDouble(data[1]), null);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        NodeArray mainPartition = quadTree.findPariton(x_coordinate, y_coordinate);
        List nearestNeighbours = findNearestNeighbours(mainPartition, x_coordinate ,y_coordinate);
        Circle circle = findFurthestNeighbourCircle( x_coordinate, y_coordinate, nearestNeighbours);

        LinkedList<NodeArray> partitons = quadTree.findPartitions(quadTree.getRootNodeArray(), new Circle(5,0,5));

//        FuncArray funcCircleOverlappingSquare = new FuncArray() {
//            @Override
//            public void call(QuadTreeArray quadTree, NodeArray node) {
//                System.out.println("NODES " + node.toString() + " Size " + node.getX() + " " + node.getW());
//            }
//        };
//
//        quadTree.traverse(quadTree.getRootNodeArray(), funcCircleOverlappingSquare);

        for ( NodeArray partition : partitons){
            findNearestNeighbours(partition, x_coordinate,y_coordinate);

        }
    }

    private static Circle findFurthestNeighbourCircle(double x_coordinate, double y_coordinate, List<Neighbour> nearestNeighbours) {
        Neighbour furthestNeighbour = nearestNeighbours.get(nearestNeighbours.size()-1);
        double radious = Math.sqrt(Math.pow(x_coordinate - furthestNeighbour.getPoint().getX(), 2) + Math.pow(y_coordinate - furthestNeighbour.getPoint().getY(), 2));
        return new Circle(x_coordinate, y_coordinate, radious);
    }

    private static List<Neighbour> findNearestNeighbours(NodeArray partition, long x_coordinate, long y_coordinate) {

        LinkedList<Neighbour> nearestNeighbours = new LinkedList<>();

        for (Point point : partition.getPoints()) {
            double distance = Math.sqrt(Math.pow(x_coordinate - point.getX(), 2) + Math.pow(y_coordinate - point.getY(), 2));
            nearestNeighbours.add(new Neighbour(point,distance));
        }

        Collections.sort(nearestNeighbours, new NeighbourComparator());

        return nearestNeighbours;
    }
}


