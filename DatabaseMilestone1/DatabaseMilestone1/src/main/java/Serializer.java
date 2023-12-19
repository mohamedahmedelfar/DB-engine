package main.java;

import java.io.*;

public class Serializer extends DBAppException{
    public static void serialize(String path, Serializable object){
        try{
            FileOutputStream fOut = new FileOutputStream(path);
            ObjectOutputStream Oout = new ObjectOutputStream(fOut);
            Oout.writeObject(object);
            Oout.close();
            fOut.close();
        } catch (IOException i){
            i.printStackTrace();
        }
    }
    public static Object deserialize(String path) throws DBAppException {
        Object output = null;
        try{
            FileInputStream fIn = new FileInputStream(path);
            ObjectInputStream oIn = new ObjectInputStream(fIn);
            output = oIn.readObject();
            oIn.close();
            fIn.close();
        } catch (IOException  | ClassNotFoundException i) {
            throw new DBAppException("Path not found");
        }
        return output;
    }

}
