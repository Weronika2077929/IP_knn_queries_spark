import java.io.*;
import java.util.ArrayList;

/**
 * Created by Wera on 29/01/2017.
 */
public class NodeArray {

    private String FILE_PATH = "C:/Users/Wera/Documents/4thyear/IP/Java_Spark_Project/src/main/resources/quadtree_data/";
    private long FILE_LENGHT = 1280;

    private double x;
    private double y;
    private double w;
    private double h;
    private NodeArray opt_parent;
    private ArrayList<Point> points;
    private NodeType nodetype = NodeType.EMPTY;
    private NodeArray nw;
    private NodeArray ne;
    private NodeArray sw;
    private NodeArray se;
    private File file;
    private String fileName;
    /**
     * Constructs a new quad tree node.
     *
     * @param {double} x X-coordiate of node.
     * @param {double} y Y-coordinate of node.
     * @param {double} w Width of node.
     * @param {double} h Height of node.
     * @param {Node}   opt_parent Optional parent node.
     * @constructor
     */
    public NodeArray(double x, double y, double w, double h, NodeArray opt_parent) {
        this.x = x;
        this.y = y;
        this.w = w;
        this.h = h;
        this.opt_parent = opt_parent;
        this.points = new ArrayList<Point>();
        this.file = null;
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    public double getW() {
        return w;
    }

    public void setW(double w) {
        this.w = w;
    }

    public double getH() {
        return h;
    }

    public void setH(double h) {
        this.h = h;
    }

    public NodeArray getParent() {
        return opt_parent;
    }

    public void setParent(NodeArray opt_parent) {
        this.opt_parent = opt_parent;
    }

    public void addPoint(Point point) {
        this.points.add(point);
        addPointToFile(point);
    }

    private void addPointToFile(Point point) {
        if(file == null) {
            this.createNewFile();
        }

        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(this.file, true));
            bw.write(point.toString());
            bw.newLine();
            bw.flush();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            } finally {
            try {
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void createNewFile() {
        this.fileName = FILE_PATH + + x + "_" + y + ".txt";
        this.file = new File(fileName);
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ArrayList<Point> getPoints() {
        return this.points;
    }

    public void setNodeArrayType(NodeType nodetype) {
        this.nodetype = nodetype;
    }

    public NodeType getNodeArrayType() {
        return this.nodetype;
    }


    public void setNw(NodeArray nw) {
        this.nw = nw;
    }

    public void setNe(NodeArray ne) {
        this.ne = ne;
    }

    public void setSw(NodeArray sw) {
        this.sw = sw;
    }

    public void setSe(NodeArray se) {
        this.se = se;
    }

    public NodeArray getNe() {
        return ne;
    }

    public NodeArray getNw() {
        return nw;
    }

    public NodeArray getSw() {
        return sw;
    }

    public NodeArray getSe() {
        return se;
    }

    public boolean isArrayFull(){
        if( points.size() >= 3 )
            return true;
        return false;
    }

    public boolean isFileFull(){
//        System.out.println(file.length());
//        if( file.length() >= FILE_LENGHT )
//            return true;
//        return false;

        if(points.size() >= 3 ){
            return true;
        }
        return false;
    }

    public long getFileSize(){
        return file.length();
    }

    public void emptyArrayList() {
        this.points = null;
    }

    public boolean nodeArrayContains( Point point ) {
        for(Point p : points){
            if(p.compareTo(point) == 0)
                return true;
        }
        return false;
    }

    public String toString(){
        String s = "";
        for( Point p : points){
            s += "("+ p.getX() + "," + p.getY() + ")";
        }
        return s;
    }

    public void deleteFile(){
        this.file.delete();
    }
    public File getFile(){
        return this.file;
    }

    public String getFileName(){
        return this.fileName;
    }
}
