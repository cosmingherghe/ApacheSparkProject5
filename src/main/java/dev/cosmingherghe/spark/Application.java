package dev.cosmingherghe.spark;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Application {

    public static void main(String[] args) {

        //Turn off INFO log entries
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);


        SparkSession spark = SparkSession.builder()
                .appName("Learning Spark SQL Dataframe API")
                .master("local")
                .getOrCreate();


        String studentsFile = "src/main/resources/students.csv";

        Dataset<Row> studentDf = spark.read().format("csv")
                .option("inferSchema", "true") // Make sure to use string version of true
                .option("header", true)
                .load(studentsFile);

        String gradeChartFile = "src/main/resources/grade_chart.csv";

        Dataset<Row> gradesDf = spark.read().format("csv")
                .option("inferSchema", "true") // Make sure to use string version of true
                .option("header", true)
                .load(gradeChartFile);

        /*
        1) we join the students data frame with the grades data from. // studentDf.join(gradesDf,
        2) this column from this table needs to be joined with this column from this table // studentDf.col("GPA").equalTo(gradesDf.col("GPA"))
        3) we can pick and choose the data that we want & can assign this to a new data frame.
         */

        Dataset<Row> filteredDf = studentDf.join(gradesDf, studentDf.col("GPA").equalTo(gradesDf.col("GPA")));

        System.out.println("\n\nTo see both of those tables joined together");
        filteredDf.show();

        // select 1
        filteredDf.select(studentDf.col("student_name"),
                            studentDf.col("favorite_book_title"),
                            gradesDf.col("letter_grade")).show();

        // select 2 (using "col" adding static org.apache.spark.sql.functions.*; ) or we can make it shorter without "col"
        filteredDf.select(col("student_name"),
                col("favorite_book_title"),
                col("letter_grade")).show();

        // filter >  showing top students, failing student and the name of those that read.
        filteredDf.filter(gradesDf.col("gpa").gt(3.0).and(gradesDf.col("gpa").lt(4.5))
                .or(gradesDf.col("gpa").equalTo(1.0)))
                .select("student_name",
                        "favorite_book_title",
                        "letter_grade").show();

        // filter > using "where" because it sounds more like SQL and some ppl would like it more.
        filteredDf.where(gradesDf.col("gpa").gt(3.0).and(gradesDf.col("gpa").lt(4.5))
                .or(gradesDf.col("gpa").equalTo(1.0)))
                .select("student_name",
                        "favorite_book_title",
                        "letter_grade").show();


        /*
        Continue using 3 new files: purchases.csv , products.csv & customers.csv
         */
        String purchasesFile = "src/main/resources/purchases.csv";

        Dataset<Row> purchasesDf = spark.read().format("csv")
                .option("inferSchema", "true") // Make sure to use string version of true
                .option("header", true)
                .load(purchasesFile);


        String productsFile = "src/main/resources/products.csv";

        Dataset<Row> productsDf = spark.read().format("csv")
                .option("inferSchema", "true") // Make sure to use string version of true
                .option("header", true)
                .load(productsFile);


        String customersFile = "src/main/resources/customers.csv";

        Dataset<Row> customersDf = spark.read().format("csv")
                .option("inferSchema", "true") // Make sure to use string version of true
                .option("header", true)
                .load(customersFile);

        System.out.println("\n\n\n\n\n Continue using: purchases.csv , products.csv & customers.csv files\n\n\n");
        Dataset<Row> joinedData = customersDf.join(purchasesDf,
                customersDf.col("customer_id").equalTo(purchasesDf.col("customer_id")))
                .join(productsDf, purchasesDf.col("product_id").equalTo(productsDf.col("product_id")));

        //show combined data frame
        joinedData.show();

        //dropping some data
        Dataset<Row> cleanJoinedData = joinedData
                .drop("favorite_website")
                .drop(purchasesDf.col("customer_id"))
                .drop(purchasesDf.col("product_id"))
                .drop("product_id");

        System.out.println("\n\n\nShow cleaned data frame");
        cleanJoinedData.show();

        // Playing with some SQL aggregate functions
        System.out.println("Show the first name and the amount of purchases that they made.");
        cleanJoinedData.groupBy("first_name").count().show();

        System.out.println("GroupBy a column \"first_name\" show: count, max, sum using static org.apache.spark.sql.functions.*; ");
        cleanJoinedData.groupBy("first_name").agg(
                count("product_name").as("number_of_purchases"),
                max("product_price").as("most_expensive"),
                sum("product_price").as("total_spent")
        ).show();

        System.out.println("GroupBy column \"first_name\" & \"product_name\" show: count, max, sum using static org.apache.spark.sql.functions.*; ");
        cleanJoinedData.groupBy("first_name", "product_name").agg(
                count("product_name").as("number_of_purchases"),
                max("product_price").as("most_expensive"),
                sum("product_price").as("total_spent")
        ).show();

    }
}