import java.io.*;
import java.util.*;
import java.util.function.Function;

/**
 * Datastructure: A point Quad Tree for representing 2D data. Each
 * region has the same ratio as the bounds for the tree.
 * <p/>
 * The implementation currently requires pre-determined bounds for data as it
 * can not rebalance itself to that degree.
 */
public class QuadTreeArray implements Serializable{

    private static String FILE_PATH_SUMMARY = "C:/Users/Wera/Documents/4thyear/IP/QuadTreeData/";
    private int count = 0;
    public long time = 0;

    private NodeArray root_;
    private int count_ = 0;

    private String summaryFilename;

    /**
     * Constructs a new quad tree.
     *
     * @param {double} minX Minimum x-value that can be held in tree.
     * @param {double} minY Minimum y-value that can be held in tree.
     * @param {double} maxX Maximum x-value that can be held in tree.
     * @param {double} maxY Maximum y-value that can be held in tree.
     */
    public QuadTreeArray(double minX, double minY, double maxX, double maxY) {
        this.root_ = new NodeArray(minX, minY, maxX - minX, maxY - minY, null);
    }

    /**
     * Returns a reference to the tree's root node.  Callers shouldn't modify nodes,
     * directly.  This is a convenience for visualization and debugging purposes.
     *
     * @return {NodeArray} The root node.
     */
    public NodeArray getRootNodeArray() {
        return this.root_;
    }

    /**
     * Sets the value of an (x, y) point within the quad-tree.
     *
     * @param {double} x The x-coordinate.
     * @param {double} y The y-coordinate.
     * @param {Object} value The value associated with the point.
     */
    public void set(double x, double y, Object value) {
        NodeArray root = this.root_;
        if (x < root.getX() || y < root.getY() || x > root.getX() + root.getW() || y > root.getY() + root.getH()) {
            throw new QuadTreeException("Out of bounds : (" + x + ", " + y + ")");
        }
        if (this.insert(root, new Point(x, y, value))) {
            this.count_++;
        }
    }

    public void set(SetOfPoints setOfPoints){
        NodeArray root = this.root_;
        if(setOfPoints.getX()<root.getX() ||
                setOfPoints.getY() < root.getY() ||
                setOfPoints.getX() > root.getX() + root.getW() ||
                setOfPoints.getY() > root.getY() + root.getH() ||
                setOfPoints.getX() + setOfPoints.getW() >root.getX() + root.getW() ||
                setOfPoints.getY() + setOfPoints.getH() > root.getY() + root.getH()){
            throw new QuadTreeException("Out of bounds");
        }
        this.insert(root,setOfPoints);
    }

    /**
     * @return {boolean} Whether the tree is empty.
     */
    public boolean isEmpty() {
        return this.root_.getNodeArrayType() == NodeType.EMPTY;
    }

    /**
     * @return {number} The number of items in the tree.
     */
    public int getCount() {
        return this.count_;
    }

    /**
     * Removes all items from the tree.
     */
    public void clear() {
        this.root_.setNw(null);
        this.root_.setNe(null);
        this.root_.setSw(null);
        this.root_.setSe(null);
        this.root_.setNodeArrayType(NodeType.EMPTY);
        this.root_.clear();
        this.count_ = 0;
    }


    public void navigate(NodeArray node, FuncArray func, double xmin, double ymin, double xmax, double ymax) {
        switch (node.getNodeArrayType()) {
            case LEAF:
                func.call(this, node);
                break;

            case POINTER:
                if (intersects(xmin, ymax, xmax, ymin, node.getNe()))
                    this.navigate(node.getNe(), func, xmin, ymin, xmax, ymax);
                if (intersects(xmin, ymax, xmax, ymin, node.getSe()))
                    this.navigate(node.getSe(), func, xmin, ymin, xmax, ymax);
                if (intersects(xmin, ymax, xmax, ymin, node.getSw()))
                    this.navigate(node.getSw(), func, xmin, ymin, xmax, ymax);
                if (intersects(xmin, ymax, xmax, ymin, node.getNw()))
                    this.navigate(node.getNw(), func, xmin, ymin, xmax, ymax);
                break;
        }
    }

    private boolean intersects(double left, double bottom, double right, double top, NodeArray node) {
        return !(node.getX() > right ||
                (node.getX() + node.getW()) < left ||
                node.getY() > bottom ||
                (node.getY() + node.getH()) < top);
    }


    /**
     * Traverses the tree depth-first, with quadrants being traversed in clockwise
     * order (NE, SE, SW, NW).  The provided function will be called for each
     * leaf node that is encountered.
     * @param {QuadTree.NodeArray} node The current node.
     * @param {function(QuadTree.NodeArray)} fn The function to call
     *     for each leaf node. This function takes the node as an argument, and its
     *     return value is irrelevant.
     * @private
     */
    public void traverse(NodeArray node, FuncArray func) {
        switch (node.getNodeArrayType()) {
            case LEAF:
                func.call(this, node);
                break;

            case POINTER:
                this.traverse(node.getNe(), func);
                this.traverse(node.getSe(), func);
                this.traverse(node.getSw(), func);
                this.traverse(node.getNw(), func);
                break;
        }
    }
    public void traverseWithStringBuilder(NodeArray node, FunctionSaveToSummary func, StringBuilder nodeSummary) {
        switch (node.getNodeArrayType()) {
            case LEAF:
                func.call(this, node,nodeSummary);
                break;

            case POINTER:
                this.traverseWithStringBuilder(node.getNe(), func, nodeSummary);
                this.traverseWithStringBuilder(node.getSe(), func, nodeSummary);
                this.traverseWithStringBuilder(node.getSw(), func, nodeSummary);
                this.traverseWithStringBuilder(node.getNw(), func, nodeSummary);
                break;
        }
    }

    public NodeArray findPariton(double x, double y){
        return findPartition(this.root_, x,y);
    }

    private NodeArray findPartition(NodeArray node, double x, double y) {
        NodeArray resposne = null;
        switch (node.getNodeArrayType()) {
            case EMPTY:
                break;

            case LEAF:
                resposne = node;
                break;

            case POINTER:
                resposne = this.findPartition(this.getQuadrantForPoint(node, x, y), x, y);
                break;

            default:
                throw new QuadTreeException("Invalid nodeType");
        }
        return resposne;
    }

    /**
     * Inserts a point into the tree, updating the tree's structure if necessary.
     * @param {.QuadTree.NodeArray} parent The parent to insert the point
     *     into.
     * @param {QuadTree.Point} point The point to insert.
     * @return {boolean} True if a new node was added to the tree; False if a node
     *     already existed with the correpsonding coordinates and had its value
     *     reset.
     * @private
     */
    private boolean insert(NodeArray parent, Point point) {
        Boolean result = false;
        switch (parent.getNodeArrayType()) {
            case EMPTY:
                this.setPointForNodeArray(parent, point);
                result = true;
                break;
            case LEAF:
                if ( parent.nodeContains(point) ) {
                    result = false;
                } else if ( parent.isFull() ) {
                    this.split(parent);
                    result = insert(parent,point);
                } else {
                    this.setPointForNodeArray(parent, point);
                    result = true;
                }
                break;
            case POINTER:
                result = this.insert(
                        this.getQuadrantForPoint(parent, point.getX(), point.getY()), point);
                break;

            default:
                throw new QuadTreeException("Invalid nodeType in parent");
        }
        return result;
    }

    private boolean insert( NodeArray parent, SetOfPoints setOfPoints){
        Boolean result = false;
        switch (parent.getNodeArrayType()) {
            case EMPTY:
                this.setPointsForNodeArray(parent, setOfPoints);
                result = true;
                break;
            case LEAF:
                if ( parent.nodeContains(setOfPoints) ) {
                    result = false;
                } else if ( parent.isFull() ) {
                    this.split(parent);
                    result = insert(parent,setOfPoints);
                } else {
                    this.setPointsForNodeArray(parent, setOfPoints);
                    result = true;
                }
                break;
            case POINTER:
                result = this.insert(
                        this.getQuadrantForSetOfPoints(parent, setOfPoints),setOfPoints);
                break;

            default:
                throw new QuadTreeException("Invalid nodeType in parent");
        }
        return result;
    }

    /**
     * Converts a leaf node to a pointer node and reinserts the node's point into
     * the correct child.
     * @param {QuadTree.NodeArray} node The node to split.
     * @private
     */
    private void split(NodeArray node) {
        HashSet<Point> oldPoints = node.getPoints();
        node.clear();

//        node.deleteFile();

        node.setNodeArrayType(NodeType.POINTER);

        double x = node.getX();
        double y = node.getY();
        double hw = node.getW() / 2;
        double hh = node.getH() / 2;

        node.setNw(new NodeArray(x, y, hw, hh, node));
        node.setNe(new NodeArray(x + hw, y, hw, hh, node));
        node.setSw(new NodeArray(x, y + hh, hw, hh, node));
        node.setSe(new NodeArray(x + hw, y + hh, hw, hh, node));

        for( Point p : oldPoints){
            this.insert(node, p);
        }
    }

    /**
     * Returns the child quadrant within a node that contains the given (x, y)
     * coordinate.
     * @param {QuadTree.NodeArray} parent The node.
     * @param {number} x The x-coordinate to look for.
     * @param {number} y The y-coordinate to look for.
     * @return {QuadTree.NodeArray} The child quadrant that contains the
     *     point.
     * @private
     */
    private NodeArray getQuadrantForPoint(NodeArray parent, double x, double y) {
        double mx = parent.getX() + parent.getW() / 2;
        double my = parent.getY() + parent.getH() / 2;
        if (x < mx) {
            return y < my ? parent.getNw() : parent.getSw();
        } else {
            return y < my ? parent.getNe() : parent.getSe();
        }
    }


    private NodeArray getQuadrantForSetOfPoints(NodeArray parent, SetOfPoints setOfPoints) {
        double mx = parent.getX() + parent.getW() / 2;
        double my = parent.getY() + parent.getH() / 2;
// TODO Check if return statements are correct
        if (setOfPoints.getX() < mx && setOfPoints.getX() + setOfPoints.getW() <= mx) {
            if ( setOfPoints.getY() < my && setOfPoints.getY() + setOfPoints.getH() <my){
                return parent.getNw();
            } else {
                return parent.getSw();
            }
        } else {
            if ( setOfPoints.getY() < my && setOfPoints.getY() + setOfPoints.getH() <my){
                return parent.getNe();
            } else {
                return parent.getSe();
            }
        }
    }


    /**
     * Sets the point for a node, as long as the node is a leaf or empty.
     * @param {QuadTree.NodeArray} node The node to set the point for.
     * @param {QuadTree.Point} point The point to set.
     * @private
     */
    private void setPointForNodeArray(NodeArray node, Point point) {
        node.setNodeArrayType(NodeType.LEAF);
        node.addPoint(point);
    }

    private void setPointsForNodeArray(NodeArray node, SetOfPoints setOfPoints){
        node.setNodeArrayType(NodeType.LEAF);
        node.addSetOfPoints(setOfPoints);
    }

    public boolean isCircleOverlappingSquare(Circle circle, NodeArray node){

        Double x_rectangleCentre = node.getX() + node.getW()/2;
        Double y_rectangleCentre = node.getY() + node.getH()/2;

        Double x_distance = Math.abs(circle.getX() - x_rectangleCentre);
        Double y_distance = Math.abs(circle.getY() - y_rectangleCentre);

        if (x_distance > (node.getW()/2 + circle.getRadius())) return false;
        if (y_distance > (node.getH()/2 + circle.getRadius())) return false;

        if (x_distance <= (node.getW()/2)) return true;
        if (y_distance <= (node.getH()/2)) return true;

        Double cornerDistance_sq = Math.pow(x_distance - node.getW()/2, 2) + Math.pow(y_distance - node.getH()/2, 2) ;

        return (cornerDistance_sq <= Math.pow(circle.getRadius(),2));
    }

    public LinkedList<NodeArray> findPartitions(NodeArray node, Circle circle, NodeArray mainPartition) {
        LinkedList<NodeArray> partitions = new LinkedList<>();
        if(isCircleOverlappingSquare(circle,node)) {
            if(node.getNodeArrayType() == NodeType.POINTER){
                partitions.addAll(findPartitions(node.getNw(),circle, null));
                partitions.addAll(findPartitions(node.getNe(),circle, null));
                partitions.addAll(findPartitions(node.getSe(),circle, null));
                partitions.addAll(findPartitions(node.getSw(),circle, null));
            } else {
                partitions.add(node);
            }
        }
        partitions.remove(mainPartition);
        return partitions;
    }

    public void saveQuadTreetoDisk(){

        FuncArray saveNodeToFile = new FuncArray() {
            @Override
            public void call(QuadTreeArray quadTree, NodeArray node) {
                node.saveToFile();
            }
        };


        traverse(this.root_, saveNodeToFile);
    }

    public void makeQuadTreeSummary() {

        StringBuilder summary = new StringBuilder();

        FunctionSaveToSummary saveNodeToSummary = new FunctionSaveToSummary() {
            @Override
            public void call(QuadTreeArray quadTree, NodeArray node, StringBuilder nodeSummary) {
                nodeSummary.append(node.getX() + " " + node.getY() + " " + node.getW() + " " +  node.getH() + " " +  node.getNodeSize()+ " " + node.getFileName() + " " +  System.getProperty("line.separator") );
            }
        };

        traverseWithStringBuilder(this.root_, saveNodeToSummary, summary);

        this.saveQuadTreeSummaryToDisk(summary);

    }

    private void saveQuadTreeSummaryToDisk(StringBuilder summary) {

        File quadTreeSummaryFile = this.createQuadTreeSummaryFile();

        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(quadTreeSummaryFile, true));
            bw.write(summary.toString());
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

    private File createQuadTreeSummaryFile() {
        this.summaryFilename = FILE_PATH_SUMMARY + "summary.txt";
        File summaryFile = new File(summaryFilename);
        try {
            summaryFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return summaryFile;
    }

    public void createQuadTreeFromSummaryFile(String summaryFilename){

        //        Skips first line in the file
        try {
            Scanner in = new Scanner(new FileReader(summaryFilename));
            while(in.hasNext()) {
                String[] data = in.nextLine().split(" ");
                SetOfPoints setOfPoints = new SetOfPoints(Double.parseDouble(data[0]),
                        Double.parseDouble(data[1]),
                        Double.parseDouble(data[2]),
                        Double.parseDouble(data[3]),
                        Double.parseDouble(data[4]),
                        data[5]);
                this.set(setOfPoints);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        this.pointAllNodesToDataFiles();
    }

    private void pointAllNodesToDataFiles() {
        FuncArray setDataFileInNode = new FuncArray() {
            @Override
            public void call(QuadTreeArray quadTree, NodeArray node) {
                Iterator<SetOfPoints> iterator = node.getPointsSets().iterator();
                node.setFileName(iterator.next().getFileName());
            }
        };

        traverse(this.root_,setDataFileInNode);
    }

}
