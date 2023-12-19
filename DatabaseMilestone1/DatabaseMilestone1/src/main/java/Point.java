package main.java;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Point implements Serializable {
    private Object x;
    private Object y;
    private Object z;
    private Vector<Page> pointer;

    public Point(Object x, Object y, Object z) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.pointer = new Vector<Page>();
    }

    public Object getX() {
        return x;
    }

    public void setX(Object x) {
        this.x = x;
    }

    public Object getY() {
        return y;
    }

    public void setY(Object y) {
        this.y = y;
    }

    public Object getZ() {
        return z;
    }

    public void setZ(Object z) {
        this.z = z;
    }

    public Vector<Page> getPointer() {
        return pointer;
    }

    public void setPointer(Vector<Page> pointer) {
        this.pointer = pointer;
    }
}
