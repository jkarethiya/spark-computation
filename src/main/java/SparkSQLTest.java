import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class SparkSQLTest {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().master("local[4]").appName("SparkSQLTest").getOrCreate();

		Dataset<Row> df = spark.read().json("src\\main\\resources\\people.json");

		// Displays the content of the DataFrame to stdout
		df.show();

		// Print the schema in a tree format
		df.printSchema();

		// Select only the "name" column
		df.select("name").show();

		// Select everybody, but increment the age by 1
		df.select(col("name"), col("age").plus(1)).show();

		// Select people older than 21
		df.filter(col("age").gt(21)).show();

		// Count people by age
		df.groupBy("age").count().show();
		
		// Register the DataFrame as a SQL temporary view
		df.createOrReplaceTempView("people");

		Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
		sqlDF.show();
	}

}
