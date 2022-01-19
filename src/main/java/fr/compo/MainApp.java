package fr.compo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.compo.functions.GroupByAddressePostal;
import fr.compo.reader.CsvFileReader;
import fr.compo.reader.TextFileReader;
import org.apache.spark.internal.config.ConfigBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import fr.compo.writer.FileWriter;

public class MainApp {
    public static void main(String[] args) {
        String inputFIle = "src/test/resources/aristide.txt";
        SparkSession sparkSession = SparkSession.builder().master("local[2]").getOrCreate();

        Config config = ConfigFactory.load();
        String inputPathStr = config.getString("3il.path.input");
        String outputPathStr = config.getString("3il.path.output");

        CsvFileReader reader = new CsvFileReader(inputPathStr,sparkSession);

        //Dataset<Row> ds = GroupByAddressePostal.builder().build().apply(lines);
        //ds.show(false);


        TextFileReader txtFR = new TextFileReader(inputFIle,sparkSession);
        CsvFileReader csvFileReader = new CsvFileReader(inputFIle,sparkSession);
        Dataset<Row> ds = csvFileReader.get();
        // Dataset<Row> ds = txtFR.get();

        ds.printSchema();

        new FileWriter(ConfigFactory.load().getString("3il.path.output")).accept(ds);
    }
}
