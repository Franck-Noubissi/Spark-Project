package fr.compo.functions;

import lombok.Builder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.Function;

import static org.apache.spark.sql.functions.col;

@Builder
public class FilterVille implements Function<Dataset<Row>, Dataset<Row>> {
    private final String ville;
    @Override
    public Dataset<Row> apply(Dataset<Row> dt) {
        return dt.filter(col("com_nom").contains(ville));
    }
}
