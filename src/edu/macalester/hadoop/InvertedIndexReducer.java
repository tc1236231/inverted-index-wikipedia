package edu.macalester.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

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
        context.write(word, new Text(fileFreq.toString()));
    }
}
