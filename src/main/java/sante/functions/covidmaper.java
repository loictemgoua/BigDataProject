package sante.functions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import sante.beans.sante;

import java.util.function.Function;


/**
 *
 */
public class covidmaper implements Function<Dataset<Row>, Dataset<sante>> {
    private final RowToPrix  parser = new RowToPrix();
    private final MapFunction<Row, sante> task = parser::apply;
    //private final MapFunction<Row, Prix> task = new RowToPrix();

    @Override
    public Dataset<sante> apply(Dataset<Row> inputDS) {
        return inputDS.map(task , Encoders.bean(sante.class)) ;
    }
}
