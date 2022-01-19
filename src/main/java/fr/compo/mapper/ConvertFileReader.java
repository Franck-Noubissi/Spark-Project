package fr.compo.mapper;

import fr.compo.model.ComplementAlimentaire;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.util.function.Function;

import static org.apache.spark.sql.functions.count;

@Slf4j
@RequiredArgsConstructor
public class ConvertFileReader implements Function<Dataset<Row>, Dataset<ComplementAlimentaire>> {
    private final RowLineToBean rowLineToBean = new RowLineToBean();
    final MapFunction<Row, ComplementAlimentaire> complementMapFunction = rowLineToBean::apply;

    @Override
    public Dataset<ComplementAlimentaire> apply(Dataset<Row> rows) {
        log.info("printing lines schema = ");
        rows.printSchema();
        rows.show();

        return rows.map(complementMapFunction, Encoders.bean(ComplementAlimentaire.class));
    }
}
