package fr.compo.functions;

import fr.compo.model.ComplementAlimentaire;
import lombok.Builder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.Function;

import static org.apache.spark.sql.functions.count;

@Builder
public class GroupByAddressePostal implements Function<Dataset<Row>, Dataset<Row>> {

        @Override
        public Dataset<Row> apply(Dataset<Row> dt) {
            return dt.groupBy("marques").agg(count("populationscibles"));
            //return null;
        }
}

