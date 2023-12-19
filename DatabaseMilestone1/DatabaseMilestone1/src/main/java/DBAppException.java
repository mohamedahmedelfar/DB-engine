package main.java;

public class DBAppException extends Exception{
    public DBAppException(){
        super();
    }
    public DBAppException(String exp){
        super(exp);
    }
}
