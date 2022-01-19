package fr.compo.reader;

import lombok.RequiredArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class TextFileReader implements Supplier<Dataset<Row>> {

    private final String inputPathStr;
    private final SparkSession sparkSession;


    @Override
    public Dataset<Row> get() {
        Metadata build = new MetadataBuilder().build();
        StructField prenom = new StructField("prenom", DataTypes.StringType,true, build);
        StructField nom = new StructField("nom", DataTypes.StringType,true, build);
        StructField age = new StructField("age", DataTypes.IntegerType,true, build);

        StructType schema = new StructType(new StructField[]{
                prenom,nom,age
        });
        try {
            Stream<Row> data= Files.lines(Paths.get(inputPathStr)).map(r -> {
                String[] lineData = r.split(";");
                return RowFactory.create(lineData[0],lineData[1],Integer.parseInt(lineData[2]));
            });

            return sparkSession.createDataFrame(data.collect(Collectors.toList()), schema);
        } catch (IOException e) {
            e.printStackTrace();
            return sparkSession.emptyDataset(Encoders.bean(Row.class));
        }
    }
}
