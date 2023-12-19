package main.java;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable {
    private String tableName;
    private String clusteringKeyName;
    private Object mins;

    private Object maxs;
    private Hashtable<String,String> htblColType;
    private Hashtable<String, String> htblColMin;
    private Hashtable<String, String> htblColMax;
    private Vector<Page> pages;

    public Table(String tableName, String clusteringKeyName, Hashtable<String,String> htblColType, Hashtable<String, String> htblColMin,
                 Hashtable<String, String> htblColMax){
        this.tableName = tableName;
        this.clusteringKeyName = clusteringKeyName;
        this.htblColType = htblColType;
        this.htblColMax = htblColMax;
        this.htblColMin = htblColMin;
        this.pages = new Vector<Page>();
    }
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getClusteringKeyName() {
        return clusteringKeyName;
    }

    public void setClusteringKeyName(String clusteringKeyName) {
        this.clusteringKeyName = clusteringKeyName;
    }


    public Hashtable<String, String> getHtblColType() {
        return htblColType;
    }

    public void setHtblColType(Hashtable<String, String> htblColType) {
        this.htblColType = htblColType;
    }

    public Hashtable<String, String> getHtblColMin() {
        return htblColMin;
    }

    public void setHtblColMin(Hashtable<String, String> htblColMin) {
        this.htblColMin = htblColMin;
    }

    public Hashtable<String, String> getHtblColMax() {
        return htblColMax;
    }

    public void setHtblColMax(Hashtable<String, String> htblColMax) {
        this.htblColMax = htblColMax;
    }

    public Vector<Page> getPages() {
        return pages;
    }

    public Object getMins() {
        return mins;
    }

    public void setMins(Object mins) {
        this.mins = mins;
    }

    public Object getMaxs() {
        return maxs;
    }

    public void setMaxs(Object maxs) {
        this.maxs = maxs;
    }

    public void setPages(Vector<Page> pages) {
        this.pages = pages;
    }
}
