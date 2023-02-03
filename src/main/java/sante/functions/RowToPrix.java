package sante.functions;

import org.apache.spark.sql.Row;
import sante.beans.sante;

import java.io.Serializable;
import java.util.function.Function;

public class RowToCovid implements Function<Row, sante> , Serializable {
    @Override
    public sante apply(Row row) {
        String reg = row.getAs("reg") ;
        String date_de_passage = row.getAs("date_de_passage") ;
        String nbre_pass_tot = row.getAs("nbre_pass_tot") ;
        String nbre_pass_tot_f = row.getAs("nbre_pass_tot_f") ;


        return sante.builder()
                .codeInsee(reg)
                .codePostal(date_de_passage)
                .libelleCommune(nbre_pass_tot)
                .niveauxPrix(nbre_pass_tot_f)
                .build();   }
}
