package ca.aretex.irex.explor.data.deces.functions.processor;

import ca.aretex.irex.explor.data.Covid.beans.ActeDeces;
import ca.aretex.irex.explor.data.Covid.functions.writer.ActeDecesWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

@Slf4j
@RequiredArgsConstructor
public class CovidStreamProcessor implements VoidFunction<JavaRDD<Covid>> {
    private final String outputPathStr;


    @Override
    public void call(JavaRDD<Covid> CovidJavaRDD) throws Exception {
        long ts = System.currentTimeMillis();
        log.info("micro-batch stored in folder={}", ts);

        if (CovidJavaRDD.isEmpty()) {
            log.info("no data found!");
            return;
        }

        log.info("data under processing...");
        final SparkSession sparkSession = SparkSession.active();


        Dataset<Covid> CovidDataset = sparkSession.createDataset(
                CovidJavaRDD.rdd(),
                Encoders.bean(Covid.class)
        );


        CovidDataset.printSchema();
        CovidDataset.show(5, false);

        log.info("nb actesDeces = {}", CovidDataset.count());


        CovidWriter<Covid> writer = new CovidWriter<>(outputPathStr + "/time=" + ts);
        writer.accept(CovidDataset);

        CovidDataset.unpersist();
        log.info("done");
    }
}
