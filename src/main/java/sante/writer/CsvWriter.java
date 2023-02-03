package sante.writer;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import sante.beans.sante;

import java.util.function.Consumer;

@RequiredArgsConstructor
public class CsvWriter implements Consumer<Dataset<sante>> {

    private  final String outputPath ;

    @Override
    public void accept(Dataset<sante> CovidDataSet) {
        CovidDataSet.write().csv(outputPath);
    }
}
