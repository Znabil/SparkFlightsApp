import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class GetRows {
	
	public Row call(String record) throws Exception {
	      String[] fields = record.split(",");
	      return RowFactory.create(fields[0], fields[1].trim());
	    }
	
}
