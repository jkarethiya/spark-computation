import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkCSVTest {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local[4]").appName("SparkSQLTest").getOrCreate();

		Dataset<Row> df = spark.read().option("header", true).format("csv").load("src\\main\\resources\\people.csv");

		// Displays the content of the DataFrame to stdout
		df.show();

		// Print the schema in a tree format
		df.printSchema();
	}

}
