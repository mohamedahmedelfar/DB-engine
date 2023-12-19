package main.java;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable {
    private String path;
    private Vector<Hashtable<String,Object>> data;
    private int noOfRows;
    private Object minVal;
    private Object maxVal;
    private String clusteringKeyName;

    public Page(){

    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Vector<Hashtable<String, Object>> getData() {
        return data;
    }

    public void setData(Vector<Hashtable<String, Object>> data) {
        this.data = data;
    }

    public int getNoOfRows() {
        return noOfRows;
    }

    public void setNoOfRows(int noOfRows) {
        this.noOfRows = noOfRows;
    }



    public Object getMinVal() {
        return minVal;
    }

    public void setMinVal(Object minVal) {
        this.minVal = minVal;
    }

    public Object getMaxVal() {
        return maxVal;
    }

    public void setMaxVal(Object maxVal) {
        this.maxVal = maxVal;
    }

    public Object getMin() {
        return minVal;
    }

    public Object getMax(){
        return maxVal;
    }
    public void setRange(Object min, Object max) {
        minVal = min;
        maxVal = max;
    }

    public String getClusteringKeyName() {
        return clusteringKeyName;
    }

    public void setClusteringKeyName(String clusteringKeyName) {
        this.clusteringKeyName = clusteringKeyName;
    }

}
