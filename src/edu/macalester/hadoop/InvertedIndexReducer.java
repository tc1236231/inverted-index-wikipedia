package edu.macalester.hadoop;

import com.google.gson.JsonObject;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

//import org.apache.log4j.Logger;

public class InvertedIndexReducer extends Reducer<Text, Text, JsonObject, NullWritable> {

    //private static final Logger logger = Logger.getLogger(InvertedIndexReducer.class);

    @Override
    public void reduce(Text word, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> fileFreq = new HashMap<>();
        //logger.debug("word = " + word.toString());

        for (Text page: values) {
            String pageString = page.toString();
            Integer count = fileFreq.getOrDefault(pageString, 0);
            fileFreq.put(pageString, count + 1);
        }

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("KeyWord", word.toString());
        jsonObject.addProperty("IndexResult", fileFreq.toString());
        // Key does not matter.
        context.write(jsonObject, NullWritable.get());
    }
}
