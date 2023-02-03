package sante.reader;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Supplier;

@RequiredArgsConstructor
public class CsvReader  implements Supplier<Dataset<Row>> {

    private  final SparkSession sparkSession ;
    private  final String inputPath ;


    @Override
    public Dataset<Row> get() {



        Dataset<Row> ds = sparkSession.read().option("delimiter" , ";")
                .option("header" , "true").csv(inputPath) ;
        ds.show(50, false);
        return ds;
    }
}
