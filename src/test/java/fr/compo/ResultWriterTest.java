package fr.compo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.compo.writer.FileWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ResultWriterTest {

    @Test
    public void ShouldWriteCsv(){
        Config config = ConfigFactory.load();
        String outputPath = config.getString("app.data.outputReader");
        String inputPath = config.getString("app.data.inputReader");

        String newFile = "";

        File dir = new File(outputPath);
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("testing")
                .getOrCreate();


        List<String> points = Arrays.asList("UK", "US", "Foo", "Bar");
        Dataset<Row> actual = sparkSession.createDataset(points, Encoders.STRING()).toDF("Country");

        FileWriter writer = new FileWriter(outputPath);
        writer.accept(actual);

        for (File f : dir.listFiles()) {
            if (f.isFile() && f.getName().endsWith(".csv")) {
                newFile = f.getName();
            }
        }

        Dataset<Row> expected = sparkSession
                .read()
                .option("delimiter", ";")
                .option( "header", "true")
                .csv(outputPath+'/'+newFile);

        assertThat(actual
                .select("Country")
                .collectAsList()
                .stream()
                .map(f -> f.getString(0)).toArray())
                .containsExactlyInAnyOrder("UK", "US", "Foo", "Bar");

    }
}
