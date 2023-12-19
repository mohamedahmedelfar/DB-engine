package main.java;

import java.io.Serializable;
import java.util.Date;
import java.util.Vector;

public class Node implements Serializable {
    private Vector<Point> Points;
    private Vector<Node> Nodes;
    private NodeRanges Boundaries;
    private int MaximumEntriesinOctreeNode;
    public Node(NodeRanges Bound,int max ){
        this.MaximumEntriesinOctreeNode = max;
        this.Points = new Vector<Point>(max);
        this.Boundaries = Bound;
        this.Nodes = new Vector<Node>(8);
    }
    public void split() throws DBAppException {
        for(int i =0; i < 8; i++){
            NodeRanges nr = new NodeRanges();
            if(i % 2 == 0){
                nr.setMinx(this.Boundaries.getMinx());
                nr.setMaxx(median(this.Boundaries.getMinx(),this.Boundaries.getMaxx()));
            }else{
                nr.setMinx(median(this.Boundaries.getMinx(),this.Boundaries.getMaxx()));
                nr.setMaxx(this.Boundaries.getMaxx());
            }
            if( i % 4 < 2){
                nr.setMiny(this.Boundaries.getMiny());
                nr.setMaxy(median(this.Boundaries.getMiny(),this.Boundaries.getMaxy()));
            }
            else{
                nr.setMiny(median(this.Boundaries.getMiny(),this.Boundaries.getMaxy()));
                nr.setMaxy(this.Boundaries.getMaxy());
            }
            if( i < 4){
                nr.setMinz(this.Boundaries.getMinz());
                nr.setMaxz(median(this.Boundaries.getMinz(),this.Boundaries.getMaxz()));
            }else{
                nr.setMinz(median(this.Boundaries.getMinz(),this.Boundaries.getMaxz()));
                nr.setMaxz(this.Boundaries.getMaxz());
            }
            Node newNode = new Node(nr, MaximumEntriesinOctreeNode);
            Nodes.add(newNode);
        }
        for(int i = 0; i < Points.size(); i++){
            Point p = Points.get(i);
            int help = which(p);
            for(int j = 0; j < p.getPointer().size(); j++){
                Nodes.get(help).insert(p.getPointer().get(j),p.getX(),p.getY(),p.getZ());
            }
        }
        Points.clear();
    }
    public void insert(Page p, Object x, Object y, Object z) throws DBAppException {
        if(this.isLeaf()){
            Point pointer = findPoint(x,y,z);
            if(pointer == null){
                Point newP = new Point(x, y, z);
                newP.setPointer(new Vector<Page>());
                newP.getPointer().add(p);
                Points.add(newP);
                if(Points.size() > MaximumEntriesinOctreeNode){
                    split();
                }
            }else{
               pointer.getPointer().add(p);
            }
        }else{
            int help = which(new Point(x,y,z));
            Nodes.get(help).insert(p,x,y,z);
        }
    }

    public Point findPoint(Object x, Object y, Object z) throws DBAppException {
        if(this.isLeaf()){
            for(int i = 0; i < Points.size(); i ++){
                if(DBApp.allCompareTo(Points.get(i).getX(), x) == 0 &&
                        DBApp.allCompareTo(Points.get(i).getY(), y) == 0 &&
                        DBApp.allCompareTo(Points.get(i).getZ(), z) == 0){
                    return Points.get(i);
                }
            }
            return null;
        }else{
            int help = which(new Point(x,y,z));
            return Nodes.get(help).findPoint(x,y,z);
        }
    }

    public Object median(Object min, Object max) throws DBAppException {
        if(min instanceof Integer && max instanceof Integer){
            Integer minI = (Integer) min;
            Integer maxI = (Integer) max;
            return (minI + maxI) / 2;
        }else if(min instanceof Double && max instanceof Double){
            Double maxD = (Double) min;
            Double minD = (Double) max;
            return (minD + maxD)/2;
        }else if (min instanceof Date && max instanceof Date){
            var minD = ((Date) min).getTime();
            var maxD = ((Date) max).getTime();
            return new Date((minD+ maxD)/2);
        }else if(min instanceof String && max instanceof String){
            String minS = (String) min;
            String maxS = (String) max;
            int minLength;
            if(minS.length()>maxS.length()){
                minLength = maxS.length();
            }else{
                minLength = minS.length();
            }
            String result = "";
            for(int i = 0; i < minLength; i++){
                char median = (char) ((minS.charAt(i) + maxS.charAt(i)) /2);
                result = result + median;
            }
            if(minS.length() > maxS.length()){
                result = result + minS.substring(minLength);
            }else{
                result = result + maxS.substring(minLength);
            }
            return result;
        }else{
            throw new DBAppException();
        }
    }

    public boolean isLeaf(){
        return Nodes.isEmpty();
    }

    public int which(Point p) throws DBAppException {
        Object MedianX = median(Boundaries.getMinx(),Boundaries.getMaxx());
        Object MedianY = median(Boundaries.getMiny(),Boundaries.getMaxy());
        Object MedianZ = median(Boundaries.getMinz(),Boundaries.getMaxz());
        Object x = p.getX();
        Object y = p.getY();
        Object z = p.getZ();
        if(DBApp.allCompareTo(z,MedianZ) <= 0 && DBApp.allCompareTo(y,MedianY) <= 0 && DBApp.allCompareTo(x,MedianX) <= 0){
            return 0;
        }
        if(DBApp.allCompareTo(z,MedianZ) <= 0 && DBApp.allCompareTo(y,MedianY) <= 0 && DBApp.allCompareTo(x,MedianX) > 0){
            return 1;
        }
        if(DBApp.allCompareTo(z,MedianZ) <= 0 && DBApp.allCompareTo(y,MedianY) > 0 && DBApp.allCompareTo(x,MedianX) <= 0){
            return 2;
        }
        if(DBApp.allCompareTo(z,MedianZ) <= 0 && DBApp.allCompareTo(y,MedianY) > 0 && DBApp.allCompareTo(x,MedianX) > 0){
            return 3;
        }
        if(DBApp.allCompareTo(z,MedianZ) > 0 && DBApp.allCompareTo(y,MedianY) <= 0 && DBApp.allCompareTo(x,MedianX) <= 0){
            return 4;
        }
        if(DBApp.allCompareTo(z,MedianZ) > 0 && DBApp.allCompareTo(y,MedianY) <= 0 && DBApp.allCompareTo(x,MedianX) > 0){
            return 5;
        }
        if(DBApp.allCompareTo(z,MedianZ) > 0 && DBApp.allCompareTo(y,MedianY) > 0 && DBApp.allCompareTo(x,MedianX) <= 0){
            return 6;
        }
        else{
            return 7;
        }
    }

}
