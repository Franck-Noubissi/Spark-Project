package fr.compo.mapper;

import fr.compo.model.ComplementAlimentaire;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.function.Function;


public class RowLineToBean implements Function<Row, ComplementAlimentaire>, Serializable {
        @Override
        public ComplementAlimentaire apply(Row line) {
            
            ComplementAlimentaire complementAlimentaire = ComplementAlimentaire.builder()
                    .idComplement(line.getAs(0))
                    .nomCommercial(line.getAs(1))
                    .marque(line.getAs(2))
                    .formeGalenique(line.getAs(3))
                    .responsableEtiquetage(line.getAs(4))
                    .doseJournaliere(line.getAs(5))
                    .modeEmploi(line.getAs(6))
                    .misesEnGarde(line.getAs(7))
                    .objectifsEffets(line.getAs(8))
                    .ingredients(line.getAs(9))
                    .populationsARisques(line.getAs(10))
                    .gamme(line.getAs(11))
                    .populationCible(line.getAs(12))
                    .aromes(line.getAs(13))
                    .population_a_risques(line.getAs(14))
                    .plantes(line.getAs(15))
                    .famillesPlantes(line.getAs(16))
                    .partiesPlantes(line.getAs(17))
                    .autresIngredients(line.getAs(18))
                    .objectifEffet(line.getAs(19))
                    .populationCible(line.getAs(20))
                    .build();
            return complementAlimentaire;
        }
}
