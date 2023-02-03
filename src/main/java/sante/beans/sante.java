package sante.beans;

import java.io.Serializable;

import lombok.*;
@AllArgsConstructor
@RequiredArgsConstructor
@Getter
@Builder
@Data

public class sante implements Serializable {

    private String reg;
    private String date_de_passage;
    private String nbre_pass_tot;
    private String nbre_pass_tot;
    private String nbre_pass_tot_h;


}