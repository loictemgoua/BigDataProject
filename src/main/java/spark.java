import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import sante.functions.PrixStatFunction;
import sante.reader.CsvReader;
import sante.writer.CsvWriter;

@Slf4j
public class spark {

    public static void main(String[] args) {

        Config config = ConfigFactory.load("app.properties") ;
        String masterUrl = config.getString("master") ;
        String appName = config.getString("appName") ;
        SparkSession spark = SparkSession.builder()
                .master(masterUrl)
                .appName(appName)
                .getOrCreate() ;
        String inputPath = config.getString("app.data.input") ;
        String outputPath = config.getString("app.data.output") ;
        PrixStatFunction prixStatFunction = new PrixStatFunction() ;
        CsvReader csvReader = new CsvReader(spark , inputPath) ;
        CsvWriter csvWriter = new CsvWriter(outputPath) ;
        csvWriter.accept(prixStatFunction.apply(csvReader.get()));

    }
}
