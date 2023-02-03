package ca.aretex.irex.explor.data.deces.functions.receiver;

import ca.aretex.irex.explor.data.deces.beans.ActeDeces;
import ca.aretex.irex.explor.data.deces.functions.parser.TextToActeDecesFunc;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class CovidReceiver implements Supplier<JavaDStream<Covid>> {
    private final JavaStreamingContext javaStreamingContext;
    private final String inputPathStr;

    private final CovidReceiver Covid = new CovidReceiver();
    private final Function<String, Covid> mapper = CovidReceiver::apply;
    private final Function<Path, Boolean> filter = p -> p.getName().endsWith(".txt");

    @Override
    public JavaDStream<Covid> get() {
        JavaPairInputDStream<LongWritable, Text> inputDStream = javaStreamingContext
                .fileStream(
                        inputPathStr,
                        LongWritable.class,
                        Text.class,
                        TextInputFormat.class,
                        filter,
                        true
                );
        return inputDStream.map(t -> t._2().toString()).map(mapper);
    }

}
