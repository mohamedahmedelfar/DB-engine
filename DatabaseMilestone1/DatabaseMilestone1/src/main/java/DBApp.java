package main.java;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp implements DBAppInterface {

    private int MaximumEntriesinOctreeNode;
    private int MaximumRowsCountinTablePage;

    public DBApp() {
        init();
    }

    public int getMaximumRowsCountinTablePage() {
        return MaximumRowsCountinTablePage;
    }

    @Override
    public void init() {
        readConfig();
    }

    @Override
    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws DBAppException {
            if (htblColNameType.size() != htblColNameMin.size() || htblColNameType.size() != htblColNameMax.size() || htblColNameMax.size() != htblColNameMin.size())
                throw new DBAppException("Incompatible");

            for (String a : htblColNameMax.keySet()) {
                if (!(htblColNameType.containsKey(a)))
                    throw new DBAppException("Invalid Columns");
            }
            for (String a : htblColNameMin.keySet()) {
                if (!(htblColNameType.containsKey(a)))
                    throw new DBAppException("Invalid Columns");
            }

            File myFile = new File("src/main/resources/data");
            String[] inMyFile = myFile.list();
            if (!(inMyFile.length == 0)) {
                for (int i = 0; i < inMyFile.length; i++) {
                    if (inMyFile[i].equals(strTableName + ".ser"))
                        throw new DBAppException("Table Exists");
                }
            }
            Table myTable = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
            Object minH = null;
            Object maxH = null;
            for (String a : htblColNameType.keySet()) {
                if(a.equals(strClusteringKeyColumn)){
                    minH = htblColNameMin.get(a);
                    maxH = htblColNameMax.get(a);
                }
            }
            myTable.setMins(minH);
            myTable.setMaxs(maxH);
        try {
            updateMetaDataFil(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
        } catch (IOException e) {
            throw new DBAppException();
        }
        Serializer.serialize("src/main/resources/data/" + strTableName + ".ser", myTable);

    }

    //Milestone2
    @Override
    public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {

    }

    public static int allCompareTo(Object a, Object b) throws DBAppException {
        if( a == null || b == null)
            throw new DBAppException("Null values");
        if (a instanceof java.lang.Integer && b instanceof java.lang.Integer) {
            Integer aa = (Integer) a;
            Integer bb = (Integer) b;
            return aa.compareTo(bb);
        } else if (a instanceof java.lang.Double && b instanceof java.lang.Double) {
            Double aa = (Double) a;
            Double bb = (Double) b;
            return aa.compareTo(bb);
        } else if (a instanceof java.lang.String && b instanceof java.lang.String) {
            String aa = (String) a;
            String bb = (String) b;
            return aa.compareTo(bb);
        } else if(a instanceof java.util.Date && b instanceof java.util.Date){
            Date aa = (Date) a;
            Date bb = (Date) b;
            return aa.compareTo(bb);
        }else{
            throw new DBAppException("Values of Different Types");
        }
    }

    public int getPageBinarySearch(int x,Object clusteringVal, Vector<Page> pages) throws DBAppException {
        int first = 0;
        int last = pages.size() - 1;
        int middle;
        while (last >= first) {
            middle = (first + last) / 2;
            if (allCompareTo(clusteringVal, pages.get(middle).getMin()) >= 0 && allCompareTo(clusteringVal,pages.get(middle).getMax()) <= 0) {
                return middle;
            } else if (allCompareTo(clusteringVal, pages.get(middle).getMax()) > 0) {
                first = middle + 1;
            } else {
                last = middle - 1;
            }
        }
        if (x == 0)
            return first; // law ba insert
        return -1; // law ba delete
    }

    public int getRowBinarySearch(int x, Object clusteringVal, String p, String clusteringName) throws DBAppException {
        Page page = (Page) Serializer.deserialize(p);

        int first = 0;
        int last = page.getData().size() - 1;
        int middle;
        while (last >= first) {
            middle = (first + last) / 2;
            Object value = page.getData().get(middle).get(clusteringName);
            if (allCompareTo(clusteringVal, value) == 0) {
                return middle;
            } else if (allCompareTo(clusteringVal, value) > 0) {
                first = middle + 1;
            } else {
                last = middle - 1;
            }
        }
        if(x == 0)
            return first;
        return -1;
    }

    public void checkDupPrimary(String tableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        String path = getTableByPath(tableName);
        Table table = (Table) Serializer.deserialize(path);
        String pk = table.getClusteringKeyName();
        Object pkObj = htblColNameValue.get(pk);
        for(int i = 0; i < table.getPages().size(); i++){
            Page p = (Page) Serializer.deserialize("src/main/resources/data/" + tableName + i + ".ser");
            for(int j = 0; j < p.getData().size(); j ++){
                if(p.getData().get(j).get(pk).equals(pkObj))
                    throw new DBAppException("PK already exsists");
            }
        }
    }
    @Override
    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        String path = getTableByPath(strTableName);
        Table table = (Table) Serializer.deserialize(path);
        checkDupPrimary(strTableName, htblColNameValue);
        try {
            checkInsertion(table, htblColNameValue);
        } catch (ParseException e) {
            throw new DBAppException();
        }
        Hashtable<String, String> colNameType = table.getHtblColType();
        for (String a : htblColNameValue.keySet()) {
            if (!colNameType.containsKey(a))
                htblColNameValue.put(a, new NullWrapper());
        }
        int page = getPageBinarySearch(0,htblColNameValue.get(table.getClusteringKeyName()), table.getPages());
        if (page >= table.getPages().size()) {
            Page newPage = new Page();
            newPage.setPath("src/main/resources/data/" + table.getTableName() + page + ".ser");
            //newPage.setRange(null,null);
            table.getPages().add(newPage);
            newPage.setData(new Vector<Hashtable<String, Object>>());
            Serializer.serialize(newPage.getPath(), newPage);
            Serializer.serialize("src/main/resources/data/" + table.getTableName() + ".ser", table);
        }
        int row = getRowBinarySearch(0,htblColNameValue.get(table.getClusteringKeyName()), table.getPages().get(page).getPath(), table.getClusteringKeyName());
        insertIntoPage(page, row, table, htblColNameValue);

    }

    public void insertIntoPage(int pageNumber, int rowNumber, Table table, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        if(table.getPages().size() <= pageNumber){
            Page p = new Page();
            Vector<Hashtable<String,Object>> data = new Vector<Hashtable<String,Object>>();
            p.setData(data);
            p.setPath("src/main/resources/data/" + table.getTableName() + pageNumber + ".ser");
            table.getPages().add(p);
            Serializer.serialize(getTableByPath(table.getTableName()),table);
            Serializer.serialize("src/main/resources/data/" + table.getTableName() + pageNumber + ".ser",p);
        }
        Page landing = (Page) Serializer.deserialize("src/main/resources/data/" + table.getTableName() + pageNumber + ".ser");
        //Page landing = table.getPages().get(pageNumber);
        Vector<Hashtable<String, Object>> data = landing.getData();
        data.add(rowNumber, htblColNameValue);
        landing.setNoOfRows(landing.getNoOfRows() + 1);
        updateRange(landing,table.getClusteringKeyName());
        Serializer.serialize("src/main/resources/data/" + table.getTableName() + pageNumber + ".ser", landing);
        if (landing.getData().size() > MaximumRowsCountinTablePage) {
            Hashtable<String, Object> overflow = landing.getData().get(landing.getData().size() - 1);
            landing.getData().remove(landing.getData().size() - 1);
            updateRange(landing, table.getClusteringKeyName());
            Serializer.serialize("src/main/resources/data/" + table.getTableName() + pageNumber + ".ser", landing);
            insertIntoPage(pageNumber + 1, 0, table, overflow);
        }

    }
    public void updateRange(Page p,String name){
        p.setMinVal(p.getData().get(0).get(name));
        p.setMaxVal(p.getData().get(p.getData().size()-1).get(name));
    }
    public String getTableByPath(String strTableName) {
        return "src/main/resources/data/" + strTableName + ".ser";
    }

    public void checkInsertion(Table table, Hashtable<String, Object> htblColNameValue) throws DBAppException, ParseException {
        String clusteringKey = table.getClusteringKeyName();
        boolean flag = false;
        Hashtable<String, String> colNameType = table.getHtblColType();
        for (String a : htblColNameValue.keySet()) {
            if (!a.equals(clusteringKey))
                flag = true;
            if (!colNameType.containsKey(a))
                throw new DBAppException();
        }
        if (!flag)
            throw new DBAppException();
        checkRangeType(table, htblColNameValue);
    }

    public void checkRangeType(Table table, Hashtable<String, Object> htblColNameValue) throws DBAppException, ParseException {
        Hashtable<String, String> minCol = table.getHtblColMin();
        Hashtable<String, String> maxCol = table.getHtblColMax();
        Hashtable<String, String> nameType = table.getHtblColType();
        for(String a : htblColNameValue.keySet()){
            if(!nameType.containsKey(a))
                throw new DBAppException();
        }
        for (String a : htblColNameValue.keySet()) {
            Object data = htblColNameValue.get(a);
            if (data instanceof String && nameType.get(a).equals("java.lang.String")) {
                String min = minCol.get(a);
                String max = maxCol.get(a);
                String parsedData = (String) data;
                if (parsedData.compareTo(min) == -1 || parsedData.compareTo(max) == 1) {
                    throw new DBAppException("out of range");
                }

            } else if (data instanceof Double && nameType.get(a).equals("java.lang.Double")) {
                Double min = Double.valueOf(minCol.get(a));
                Double max = Double.valueOf(maxCol.get(a));
                Double parsedData = (Double) data;
                if (parsedData.compareTo(min) == -1 || parsedData.compareTo(max) == 1) {
                    throw new DBAppException("out of range");
                }

            } else if (data instanceof Integer && nameType.get(a).equals("java.lang.Integer")) {
                Integer min = Integer.valueOf(minCol.get(a));
                Integer max = Integer.valueOf(maxCol.get(a));
                Integer parsedData = (Integer) data;
                if (parsedData.compareTo(min) == -1 || parsedData.compareTo(max) == 1) {
                    throw new DBAppException("out of range");
                }

            } else if (data instanceof Date && nameType.get(a).equals("java.util.Date")) {
                Date min = new SimpleDateFormat("yyyy-MM-dd").parse(minCol.get(a));
                Date max = new SimpleDateFormat("yyyy-MM-dd").parse(maxCol.get(a));
                Date parsedData = (Date) data;
                if (parsedData.compareTo(min) == -1 || parsedData.compareTo(max) == 1) {
                    throw new DBAppException("out of range");
                }

            } else {
                throw new DBAppException("Invalid data type");
            }
        }
    }

    @Override
    public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        String path = getTableByPath(strTableName);
        Table table = (Table) Serializer.deserialize(path);
        try {
            checkInsertion(table,htblColNameValue);
        } catch (Exception e) {
            throw new DBAppException();
        }
        int pageNo = getPageBinarySearch(1,htblColNameValue.get(table.getClusteringKeyName()), table.getPages());
        if(pageNo == -1){
            throw new DBAppException();
        }
        Page targetP = (Page) Serializer.deserialize(table.getPages().get(pageNo).getPath());
        Object a;
        Hashtable<String, String> colNT = table.getHtblColType();
        String type = table.getHtblColType().get(table.getClusteringKeyName());
        switch (type){
            case "java.lang.Integer": a = Integer.parseInt(strTableName);break;
            case "java.lang.Double" : a = Double.parseDouble(strClusteringKeyValue);break;
            case "java.lang.String" : a = strClusteringKeyValue;break;
            default :
                try {
                    a = new SimpleDateFormat("yyyy-MM-dd").parse(strClusteringKeyValue);
                } catch (ParseException e) {
                    throw new DBAppException();
                }
        }
        int rowNo = getRowBinarySearch(1,strClusteringKeyValue,targetP.getPath(), table.getClusteringKeyName());
        if(rowNo == -1){
            throw new DBAppException();
        }
        for(String aa : htblColNameValue.keySet()){
           targetP.getData().get(rowNo).replace(aa,htblColNameValue.get(aa));
        }
        updateRange(targetP,table.getClusteringKeyName());
        Serializer.serialize(table.getPages().get(pageNo).getPath(),targetP);
        Serializer.serialize(getTableByPath(table.getTableName()),table);
    }


    @Override
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        String path = getTableByPath(strTableName);
        Table table = (Table) Serializer.deserialize(path);
        try {
            checkRangeType(table, htblColNameValue);
        } catch (ParseException e) {
            throw new DBAppException();
        }
        String clusteringKey = table.getClusteringKeyName();
        if(htblColNameValue.containsKey(clusteringKey)){
            Object clusteringObj = htblColNameValue.get(clusteringKey);
            int pageNo = getPageBinarySearch(1, clusteringObj, table.getPages());
            if(pageNo == -1){
                throw new DBAppException("No Page");
            }
            Page targetP = (Page) Serializer.deserialize(table.getPages().get(pageNo).getPath());
            int rowNo = getRowBinarySearch(1,clusteringObj,targetP.getPath(),clusteringKey);
            if(rowNo == -1){
                throw new DBAppException("No row");
            }
            targetP.getData().remove(rowNo);
            updateRange(targetP, clusteringKey);
            if(targetP.getData().size() == 0){
                table.getPages().remove(targetP);
                File f = new File(targetP.getPath());
                f.delete();
                Serializer.serialize(path,table);
            }else{
                Serializer.serialize(targetP.getPath(),targetP);
                Serializer.serialize(path,table);
            }


        }else{
            Vector<Page> myPages = table.getPages();
            for(int i = 0; i <myPages.size(); i++){
                Page thisPage = myPages.get(i);
                Serializer.serialize(thisPage.getPath(),thisPage);
                Vector<Hashtable<String,Object>> data = thisPage.getData();
                for(int j = 0; j < data.size(); j++){
                    Hashtable<String,Object> record = data.get(j);
                    for(int z = 0; z < htblColNameValue.size(); z++){
                        if (allCompareTo(record.get(z), htblColNameValue.get(z)) == 0) {

                            thisPage.getData().remove(j);
                            Serializer.serialize(thisPage.getPath(),thisPage);
                            updateRange(thisPage,clusteringKey);
                        }
                    }
                }
            }
            Serializer.serialize(getTableByPath(table.getTableName()),table);
        }
    }

    //Milestone2
    @Override
    public Iterator selectFromTable() throws DBAppException {
        return null;
    }

    public void readConfig() {
        String path = "src/main/resources/DBApp.config";

        try {
            BufferedReader br = new BufferedReader(new FileReader(path));
            StringTokenizer st = new StringTokenizer(br.readLine());
            st.nextToken();
            st.nextToken();
            MaximumRowsCountinTablePage = Integer.parseInt(st.nextToken());
            st = new StringTokenizer(br.readLine());
            st.nextToken();
            st.nextToken();
            MaximumEntriesinOctreeNode = Integer.parseInt(st.nextToken());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("Config file not found");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void updateMetaDataFil(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws DBAppException, IOException {
        FileReader fr = new FileReader("src/main/resources/metadata.csv");
        BufferedReader br = new BufferedReader(fr);
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            sb.append(line).append('\n');
        }
        FileWriter fw = new FileWriter("src/main/resources/metadata.csv");
        for (String a : htblColNameType.keySet()) {
            sb.append(strTableName).append(",").append(a).append(",").append(htblColNameType.get(a)).append(",");
            boolean flag = true;
            if (a.equals(strClusteringKeyColumn)) {
                flag = true;
            } else {
                flag = false;
            }
            sb.append(flag + "," + htblColNameMin.get(a)).append(",").append(htblColNameMax.get(a)).append("\n");
        }
        fw.write(sb.toString());
        fw.flush();
        fw.close();

    }

    public static void main(String[] args) throws DBAppException, ParseException {
        DBApp test = new DBApp();
        String tableName = "Grades";
        Hashtable htblColNameType = new Hashtable<String,String>();
        htblColNameType.put("ID", "java.lang.Integer");
        htblColNameType.put("Name", "java.lang.String");
        htblColNameType.put("GPA", "java.lang.Double");
        htblColNameType.put("Date", "java.util.Date");
        Hashtable htblColNameMin = new Hashtable<String,String>();
        htblColNameMin.put("ID", "1");
        htblColNameMin.put("Name", "a");
        htblColNameMin.put("GPA", "0.0");
        htblColNameMin.put("Date", "2000-01-01");
        Hashtable htblColNameMax = new Hashtable<String,String>();
        htblColNameMax.put("ID", "10");
        htblColNameMax.put("Name", "ZZZZZZZZZZZ");
        htblColNameMax.put("GPA", "4.0");
        htblColNameMax.put("Date", "2090-01-01");
        test.createTable(tableName,"ID",htblColNameType,htblColNameMin,htblColNameMax);
        Hashtable v = new Hashtable<String,Object>();
        v.put("ID", new Integer(1));
        v.put("Name", "Hassan");
        v.put("GPA", new Double(2.1));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = sdf.parse("2003-01-01");
        v.put("Date",date );
        test.insertIntoTable(tableName,v);
        Hashtable v1 = new Hashtable<String,Object>();
        v1.put("ID", new Integer(2));
        v1.put("Name", new String("Mohamed"));
        v1.put("GPA", new Double(3.2));
        v1.put("Date", date);
        test.insertIntoTable(tableName,v1);
        Hashtable v2 = new Hashtable<String,Object>();
        v2.put("ID", new Integer(3));
        v2.put("Name", new String("Salah"));
        v2.put("GPA", new Double(3.1));
        v2.put("Date", date);
        test.insertIntoTable(tableName,v2);
        Hashtable v3 = new Hashtable<String,Object>();
        v3.put("ID", new Integer(4));
        v3.put("Name", new String("Mohsen"));
        v3.put("GPA", new Double(1.12));
        v3.put("Date", date);
        test.insertIntoTable(tableName,v3);
        // to view whats in the table
        Page my = (Page) Serializer.deserialize("src/main/resources/data/Grades0.ser");
        for(int i = 0; i < my.getData().size(); i++){
            System.out.println(my.getData().get(i));
        }

    }
}
