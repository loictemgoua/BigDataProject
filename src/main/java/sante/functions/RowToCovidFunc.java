package sante.functions;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import sante.beans.sante;

public class RowToCovidFunc  implements Function<Row,sante>, SerialiZale {
    @Override
    public Dataset<sante> apply(Dataset<Row> rowDataset) {
        Dataset<sante> cleanDs = new PrixMapper().apply(rowDataset) ;
        cleanDs.printSchema();
        cleanDs.show(5, false);
        return cleanDs;
    }
}
