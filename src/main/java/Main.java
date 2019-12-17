import org.apache.spark.*;

public class Main {

    public static void main(String[] args) {
    	SparkConf conf = new SparkConf();
    	SparkContext context =new SparkContext();
        System.out.println("Hello World!");
        DataProvider dataprovider = new DataProvider();
        dataprovider.getCSV("/media/nabil/HDD/BigData/spark project/DATA/1987.csv.bz2","/media/nabil/HDD/BigData/spark project/DATA/1987.csv");
    }
}
