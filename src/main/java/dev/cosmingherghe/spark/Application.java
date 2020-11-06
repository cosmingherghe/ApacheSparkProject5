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
    }
}