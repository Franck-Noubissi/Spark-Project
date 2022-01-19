package fr.compo;

import fr.compo.functions.GroupByAddressePostal;
import fr.compo.reader.CsvFileReader;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GroupByAdressePostalTest {

    @Test
    public void groupByTest(){
        SparkConf sparkconf = new SparkConf().setMaster("local[2]").setAppName("SparkName");
        SparkSession sparkSession = SparkSession.builder().config(sparkconf).getOrCreate();

        CsvFileReader csvFileReader = new CsvFileReader("src/main/resources/dataInput/centres_vaccination.csv",sparkSession);
        Dataset<Row> actual = csvFileReader.get();

        Dataset<Row> ds = GroupByAddressePostal.builder().build().apply(actual);

        assertThat(ds.first().toString()).contains("75007");
    }
}
