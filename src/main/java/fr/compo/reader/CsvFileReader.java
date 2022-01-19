package fr.compo.reader;

import fr.compo.model.ComplementAlimentaire;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class CsvFileReader implements Supplier<Dataset<Row>> {

    private final String inputPathStr;
    private final SparkSession sparkSession;

    public CsvFileReader(String inputPathStr){

        this.inputPathStr=inputPathStr;
        this.sparkSession=SparkSession.builder().config(new SparkConf().setMaster("local[2]").setAppName("ComplementAlimentaire")).getOrCreate();

    }

    @Override
    public Dataset<Row> get() {
        log.info("reading file at inputPathStr={}", inputPathStr);
        try {
            StructType schema = new StructType()
                    .add("idComplement", "int")
                    .add("nomCommercial", "string")
                    .add("marque", "string")
                    .add("formeGalenique", "string")
                    .add("responsableEtiquetage", "string")
                    .add("doseJournaliere", "string")
                    .add("modeEmploi", "string")
                    .add("misesEnGarde", "string")
                    .add("objectifsEffets", "string")
                    .add("Ingredients", "string")
                    .add("populationsARisque", "string")
                    .add("gamme", "string")
                    .add("populationsCibles", "string")
                    .add("aromes", "string")
                    .add("population_a_risques", "string")
                    .add("plantes", "string")
                    .add("famillesPlantes", "string")
                    .add("partiesPlantes", "string")
                    .add("autresIngredients", "string")
                    .add("objectifEffet", "string")
                    .add("population_cible", "string");

            Dataset<Row> rows = sparkSession
                    .read()
                    .option("delimiter", ";")
                    .option( "header", "true")
                    .schema(schema)
                    .option("escape","\"")
                    .option("mode", "DROPMALFORMED")
                    .csv(inputPathStr);
            return rows;
        } catch (Exception exception) {
            log.error("failed to read file at inputPathStr={} due to ...", inputPathStr, exception);
        }
        return sparkSession.emptyDataFrame();
    }
}
