import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.record.Record;
import org.apache.parquet.it.unimi.dsi.fastutil.Function;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class Main {
	
	
	public static Row call(String record) {
		String[] fields = record.split(",");
	      
	      return RowFactory.create(fields[0], fields[1].trim());
}
    public static void main(String[] args) {
    //	SparkConf conf = new SparkConf();
    	//JavaSparkContext jsc;
        System.out.println("Hello World!");
        DataProvider dataprovider = new DataProvider();
        //dataprovider.getCSV("/media/nabil/HDD/BigData/spark project/DATA/1987.csv.bz2","/media/nabil/HDD/BigData/spark project/DATA/1987.csv");
   
     //  dataprovider.getCSV("/Users/irene/Desktop/bigData/DATA/1987.csv.bz2","/Users/irene/Desktop/bigData/DATA/1987.csv");
        

        
        SparkConf sparkConf = new SparkConf().setAppName("CSVRead").setMaster("local[*]");

        
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> data=sc.textFile("/Users/irene/Desktop/bigData/DATA/1987.csv");
        
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
        //schema field name
        String[] schemaString =data.first().split(","); 
        
        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName: schemaString) {
          fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
        }
        
        StructType schema = DataTypes.createStructType(fields);
        
        //schema rows record
    
        data.zipWithIndex();
        
        JavaRDD<String[]> rddOfArrays =data.map(line -> line.split(","));
        JavaRDD<Row> rddOfRows = rddOfArrays.map(x -> RowFactory.create(x));

        
        Dataset<Row> df = sqlContext.createDataFrame(rddOfRows, schema);
        
        df.printSchema();
        df.show();
        
        
       /* for(String line:data.collect()){
            System.out.println(line);
        }
   */
      
    }
}
