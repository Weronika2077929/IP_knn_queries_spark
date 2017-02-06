import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Wera on 29/01/2017.
 */
public class QuadTreePopulator {

    private static String FILE_PATH = "C:/Users/Wera/Documents/4thyear/IP/Java_Spark_Project/src/main/resources/";
    private static String FILE_NAME = FILE_PATH + "10000";
    private static long x_coordinate = 0;
    private static long y_coordinate = 0;
    private static int k = 5;

    public static void main( String[] args ){

        try {
            FileUtils.cleanDirectory(new File(FILE_PATH + "quadtree_data/"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        QuadTreeArray quadTree = new QuadTreeArray(0,0, 1000000, 1000000);

        try {
            Scanner in = new Scanner(new FileReader(FILE_NAME));
            while(in.hasNext()) {
                String[] data = in.nextLine().split(",");
                quadTree.set(Double.parseDouble(data[0]), Double.parseDouble(data[1]), null);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        NodeArray partition = quadTree.findPariton(x_coordinate, y_coordinate);
        System.out.println( partition.getX() + " " + partition.getY());
        findNearestNeighbors(partition, x_coordinate,y_coordinate, k);

//        quadTree.set(1,1, null);
//        quadTree.set(1,2, null);
//        quadTree.set(1,4, null);
//        quadTree.set(1,3, null);
//        quadTree.set(2,2, null);
//        quadTree.set(5,10, null);
//        quadTree.set(10,1, null);
//        quadTree.set(1,5, null);
//        quadTree.set(2,1, null);
//        quadTree.set(3,1, null);
//        quadTree.set(5,1, null);
//        quadTree.set(6,1, null);
//        quadTree.set(7,1, null);
//        quadTree.set(8,1, null);
//        quadTree.set(9,1, null);
//        quadTree.set(2,3, null);
//        quadTree.set(3,2, null);
//        quadTree.set(5,3, null);
//        quadTree.set(6,2, null);
//        quadTree.set(7,3, null);
//        quadTree.set(8,2, null);
//        quadTree.set(9,3, null);

//
//        System.out.println();
//
//        quadTree.traverse(quadTree.getRootNodeArray(), new FuncArray() {
//            @Override
//            public void call(QuadTreeArray quadTree, NodeArray node) {
//                System.out.println(node.getPoints().toString());
////                System.out.println(node.getFileSize());
//            }
//        });
    }

    private static void findNearestNeighbors(NodeArray partition, long x_coordinate, long y_coordinate, int k) {

        LinkedHashMap<Point, Double> nearestNeighbours = new LinkedHashMap<>();

        for (Point point : partition.getPoints()) {
            double distance = Math.sqrt(Math.pow(x_coordinate - point.getX(), 2) + Math.pow(y_coordinate - point.getY(), 2));
            nearestNeighbours.put(point, distance);
        }

        LinkedHashMap<Point, Double> sortedMap = nearestNeighbours.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));

        System.out.println();

        int i = 0;
        for(Map.Entry<Point, Double> entry: sortedMap.entrySet()) {
            System.out.println("x: " + entry.getKey().getX()
                + ", y: " + entry.getKey().getY()
                + ", distance: " + entry.getValue());

            i++;
            if(i == k) {
                break;
            }
        }

        System.out.println(sortedMap.size());
    }
}


