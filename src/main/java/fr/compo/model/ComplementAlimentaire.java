package fr.compo.model;

import lombok.*;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
public class ComplementAlimentaire implements Serializable {
    private Integer idComplement;
    private String nomCommercial;
    private String marque;
    private String formeGalenique;
    private String responsableEtiquetage;
    private String doseJournaliere;
    private String modeEmploi;
    private String misesEnGarde;
    private String objectifsEffets;
    private String ingredients;
    private String populationsARisques;
    private String gamme;
    private String populationsCible;
    private String aromes;
    private String population_a_risques;
    private String plantes;
    private String famillesPlantes;
    private String partiesPlantes;
    private String autresIngredients;
    private String objectifEffet;
    private String populationCible;

}
