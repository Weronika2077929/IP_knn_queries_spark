import java.io.Serializable;

/**
 * Created by Wera on 23/02/2017.
 */
public class SetOfPoints implements Serializable {

    private double x;
    private double y;
    private double w;
    private double h;
    private double size;
    private String fileName;

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public SetOfPoints(double x, double y, double w, double h, double size, String fileName) {
        this.x = x;
        this.y = y;
        this.w = w;
        this.h = h;
        this.size = size;
        this.fileName = fileName;
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

    public double getSize() {
        return size;
    }

    public void setSize(double size) {
        this.size = size;
    }

    @Override
    public String toString() {
        return "SetOfPoints{" +
                "x=" + x +
                ", y=" + y +
                ", w=" + w +
                ", h=" + h +
                ", size=" + size +
                ", fileName='" + fileName + '\'' +
                "} \n";
    }
}
