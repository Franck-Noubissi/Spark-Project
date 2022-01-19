package fr.compo.writer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
public class FileWriter implements Consumer<Dataset<Row>> {

    private final String outputPathStr;

    @Override
    public void accept(Dataset<Row> element) {

        try {
//            element.coalesce(2).write().mode(SaveMode.Overwrite).csv(outputPathStr);
            element.coalesce(1).write().csv(outputPathStr);

        } catch (Exception e) {
            e.printStackTrace();
        }



    }
}

