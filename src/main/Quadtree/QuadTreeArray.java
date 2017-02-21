import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.function.Function;

/**
 * Datastructure: A point Quad Tree for representing 2D data. Each
 * region has the same ratio as the bounds for the tree.
 * <p/>
 * The implementation currently requires pre-determined bounds for data as it
 * can not rebalance itself to that degree.
 */
public class QuadTreeArray implements Serializable{
    private int count = 0;
    public long time = 0;

    private NodeArray root_;
    private int count_ = 0;

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

    /**
     * Gets the value of the point at (x, y) or null if the point is empty.
     *
     * @param {double} x The x-coordinate.
     * @param {double} y The y-coordinate.
     * @param {Object} opt_default The default value to return if the node doesn't
     *                 exist.
     * @return {*} The value of the node, the default value if the node
     *         doesn't exist, or undefined if the node doesn't exist and no default
     *         has been provided.
     */
//    public Object get(double x, double y, Object opt_default) {
//        NodeArray node = this.find(this.root_, x, y);
//        return node != null ? node.getPoint().getValue() : opt_default;
//    }

    /**
     * Removes a point from (x, y) if it exists.
     *
     * @param {double} x The x-coordinate.
     * @param {double} y The y-coordinate.
     * @return {Object} The value of the node that was removed, or null if the
     *         node doesn't exist.
     */
//    public Object remove(double x, double y) {
//        NodeArray node = this.find(this.root_, x, y);
//        if (node != null) {
//            Object value = node.getPoint().getValue();
//            node.setPoint(null);
//            node.setNodeArrayType(NodeType.EMPTY);
//            this.balance(node);
//            this.count_--;
//            return value;
//        } else {
//            return null;
//        }
//    }

    /**
     * Returns true if the point at (x, y) exists in the tree.
     *
     * @param {double} x The x-coordinate.
     * @param {double} y The y-coordinate.
     * @return {boolean} Whether the tree contains a point at (x, y).
     */
//    public boolean contains(double x, double y) {
//        return this.get(x, y, null) != null;
//    }

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
//        this.root_.clear();
        this.count_ = 0;
    }

    /**
     * Returns an array containing the coordinates of each point stored in the tree.
     * @return {Array.<Point>} Array of coordinates.
     */
//    public Point[] getKeys() {
//        final List<Point> arr = new ArrayList<Point>();
//        this.traverse(this.root_, new FuncArray() {
//            @Override
//            public void call(QuadTreeArray quadTree, NodeArray node) {
//                arr.add(node.getPoint());
//            }
//        });
//        return arr.toArray(new Point[arr.size()]);
//    }

    /**
     * Returns an array containing all values stored within the tree.
     * @return {Array.<Object>} The values stored within the tree.
     */

//    public Object[] getValues() {
//        final List<Object> arr = new ArrayList<Object>();
//        this.traverse(this.root_, new FuncArray() {
//            @Override
//            public void call(QuadTreeArray quadTree, NodeArray node) {
//                arr.add(node.getPoint().getValue());
//            }
//        });
//
//        return arr.toArray(new Object[arr.size()]);
//    }

//    public Point[] searchIntersect(final double xmin, final double ymin, final double xmax, final double ymax) {
//        final List<Point> arr = new ArrayList<Point>();
//        this.navigate(this.root_, new FuncArray() {
//            @Override
//            public void call(QuadTreeArray quadTree, NodeArray node) {
//                Point pt = node.getPoint();
//                if (pt.getX() < xmin || pt.getX() > xmax || pt.getY() < ymin || pt.getY() > ymax) {
//                    // Definitely not within the polygon!
//                } else {
//                    arr.add(node.getPoint());
//                }
//
//            }
//        }, xmin, ymin, xmax, ymax);
//        return arr.toArray(new Point[arr.size()]);
//    }

//    public Point[] searchWithin(final double xmin, final double ymin, final double xmax, final double ymax) {
//        final List<Point> arr = new ArrayList<Point>();
//        this.navigate(this.root_, new FuncArray() {
//            @Override
//            public void call(QuadTreeArray quadTree, NodeArray node) {
//                Point pt = node.getPoint();
//                if (pt.getX() > xmin && pt.getX() < xmax && pt.getY() > ymin && pt.getY() < ymax) {
//                    arr.add(node.getPoint());
//                }
//            }
//        }, xmin, ymin, xmax, ymax);
//        return arr.toArray(new Point[arr.size()]);
//    }

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
     * Clones the quad-tree and returns the new instance.
     * @return {QuadTree} A clone of the tree.
     */
//    public QuadTreeArray clone() {
//        double x1 = this.root_.getX();
//        double y1 = this.root_.getY();
//        double x2 = x1 + this.root_.getW();
//        double y2 = y1 + this.root_.getH();
//        final QuadTreeArray clone = new QuadTreeArray(x1, y1, x2, y2);
//        // This is inefficient as the clone needs to recalculate the structure of the
//        // tree, even though we know it already.  But this is easier and can be
//        // optimized when/if needed.
//        this.traverse(this.root_, new Func() {
//            @Override
//            public void call(QuadTreeArray quadTree, NodeArray node) {
//                clone.set(node.getPoint().getX(), node.getPoint().getY(), node.getPoint().getValue());
//            }
//        });
//
//
//        return clone;
//    }

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
//                System.out.println(node.getX() + " " + node.getY() + " " + node.getW() + " " + node.getH());
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
        long start = System.nanoTime();
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
        long end = System.nanoTime();
        time += end - start;
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
     * Attempts to balance a node. A node will need balancing if all its children
     * are empty or it contains just one leaf.
     * @param {QuadTree.NodeArray} node The node to balance.
     * @private
     */
//    private void balance(NodeArray node) {
//        switch (node.getNodeArrayType()) {
//            case EMPTY:
//            case LEAF:
//                if (node.getParent() != null) {
//                    this.balance(node.getParent());
//                }
//                break;
//
//            case POINTER: {
//                NodeArray nw = node.getNw();
//                NodeArray ne = node.getNe();
//                NodeArray sw = node.getSw();
//                NodeArray se = node.getSe();
//                NodeArray firstLeaf = null;
//
//                // Look for the first non-empty child, if there is more than one then we
//                // break as this node can't be balanced.
//                if (nw.getNodeArrayType() != NodeType.EMPTY) {
//                    firstLeaf = nw;
//                }
//                if (ne.getNodeArrayType() != NodeType.EMPTY) {
//                    if (firstLeaf != null) {
//                        break;
//                    }
//                    firstLeaf = ne;
//                }
//                if (sw.getNodeArrayType() != NodeType.EMPTY) {
//                    if (firstLeaf != null) {
//                        break;
//                    }
//                    firstLeaf = sw;
//                }
//                if (se.getNodeArrayType() != NodeType.EMPTY) {
//                    if (firstLeaf != null) {
//                        break;
//                    }
//                    firstLeaf = se;
//                }
//
//                if (firstLeaf == null) {
//                    // All child nodes are empty: so make this node empty.
//                    node.setNodeArrayType(NodeType.EMPTY);
//                    node.setNw(null);
//                    node.setNe(null);
//                    node.setSw(null);
//                    node.setSe(null);
//
//                } else if (firstLeaf.getNodeArrayType() == NodeType.POINTER) {
//                    // Only child was a pointer, therefore we can't rebalance.
//                    break;
//
//                } else {
//                    // Only child was a leaf: so update node's point and make it a leaf.
//                    node.setNodeArrayType(NodeType.LEAF);
//                    node.setNw(null);
//                    node.setNe(null);
//                    node.setSw(null);
//                    node.setSe(null);
//                    node.setPoint(firstLeaf.getPoint());
//                }
//
//                // Try and balance the parent as well.
//                if (node.getParent() != null) {
//                    this.balance(node.getParent());
//                }
//            }
//            break;
//        }
//    }

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
}
