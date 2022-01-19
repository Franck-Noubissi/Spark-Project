package fr.compo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.junit.Test;

public class textUT {
    @Test
    public void test(){
        String inputFIle = "src/test/resources/aristide.txt";
        SparkSession sparkSession = SparkSession.builder().master("local[2]").getOrCreate();

        Metadata build = new MetadataBuilder().build();
        StructField prenom = new StructField("prenom", DataTypes.StringType,true, build);
        StructField nom = new StructField("nom", DataTypes.StringType,true, build);

        StructType schema = new StructType(new StructField[]{
                prenom,nom
        });
        Dataset<String> data = sparkSession.read().option("delimiter",";").schema(schema).textFile(inputFIle);



    }
}
