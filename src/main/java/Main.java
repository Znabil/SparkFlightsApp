import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;


public class Main {

    public static void main(String[] args) {
//    	SparkConf conf = new SparkConf().setAppName("SparkFlightApp").setMaster("spark://nabil-HP-EliteBook-8470p:7077");
//    	JavaSparkContext sc = new JavaSparkContext(conf);
    	SparkSession spark = SparkSession
    			   .builder()
    			   .appName("SparkFlightsApp")
    			   .master("spark://nabil-HP-EliteBook-8470p:7077")
    			   .getOrCreate();
        System.out.println("Starting the job .....");
//        DataProvider dataprovider = new DataProvider();
//        String str = dataprovider.getCSV("/media/nabil/HDD1/BigData/spark project/DATA/1987.csv.bz2","/media/nabil/HDD1/BigData/spark project/DATA/1987.csv");
        File file = new File("/media/nabil/HDD1/BigData/spark project/DATA/1987/1987.csv");
        String srcData = "/media/nabil/HDD1/BigData/spark project/DATA/1987/1987.csv";
        
        Dataset ds = spark
        		  .read()
        		  .format("csv")
        		  .option("header", "true")
        		  .load(srcData);
        ds.show();
 //       JavaRDD<String> data= spark.read(ds);
        
//        JavaRDD<String> data= sc.textFile(srcData);
//
//       // distFile.saveAsTextFile("/media/nabil/HDD/BigData/spark project/DATA/1987");
//        String[] schemaString =   data.first().split(","); 
//        
//        List<StructField> fields = new ArrayList<StructField>();
//        for (String fieldName: schemaString) {
//          fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
//        }
//        
//        StructType schema = DataTypes.createStructType(fields);
//        
//        //schema rows record
//    
//        data.zipWithIndex();
//        
//        JavaRDD<String[]> rddOfArrays =data.map(line -> line.split(","));
//        JavaRDD<Row> rddOfRows = rddOfArrays.map(x -> RowFactory.create(x));
//
//        SparkSession spark = SparkSession.builder().getOrCreate();
//
//        Dataset<Row> df = new spark.createDataFrame(rddOfRows, schema);
//        
//        df.printSchema();
//        df.show();
        
//        Scanner scan = null;
       
//		try {
//			scan = new Scanner(file);
//			int k = 1 ;
//			while (scan.hasNext()) {
//				
//				k++;
//                String  i = scan.next();
//                System.out.println(k+"  "+i);
//                }
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//            
//            sc.close();
		
    }
}
