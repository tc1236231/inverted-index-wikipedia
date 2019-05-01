package edu.macalester.hadoop;

import com.google.gson.JsonObject;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;

public class InvertedIndexReducer extends Reducer<Text, Text, JsonObject, NullWritable> {

    private static final transient Logger logger = Logger.getLogger(InvertedIndexReducer.class);

    @Override
    public void reduce(Text word, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> fileFreq = new HashMap<>();
        //logger.debug("word = " + word.toString());

        for (Text page: values) {
            String pageString = page.toString();
            Integer count = fileFreq.getOrDefault(pageString, 0);
            fileFreq.put(pageString, count + 1);
        }

        String KeyWordStr = word.toString();
        String IndexResultStr = fileFreq.toString();

        if((KeyWordStr.getBytes().length + IndexResultStr.getBytes().length) >= (104857600 - 2048))
        {
            logger.info("MY_LOGGING: Index Data is too big to store in BigQuery -> IGNORED");
            logger.info("MY_LOGGING: KeyWord: " + KeyWordStr);
            return;
        }

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("KeyWord", word.toString());
        jsonObject.addProperty("IndexResult", fileFreq.toString());

        // Key does not matter.
        context.write(jsonObject, NullWritable.get());
    }
}
