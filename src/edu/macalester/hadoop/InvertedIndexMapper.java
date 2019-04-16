package edu.macalester.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.StringTokenizer;


public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    //private static final transient Logger logger = Logger.getLogger("app");
    private Text word = new Text();


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // Returns  String[0] = <title>[TITLE]</title>
        //          String[1] = <text>[CONTENT]</text>
        // !! without the <tags>.
        String[] titleAndText = parseTitleAndText(value);

        String pageString = titleAndText[0];
        String text = titleAndText[1]
                .replaceAll("[^\\w\\s]|('s|ly|ed|ing|ness|.|,|\\?|'|:|;) ", " ")
                .toLowerCase();

        if(notValidPage(pageString))
            return;

        Text page = new Text(pageString.replace(' ', '_'));
        StringTokenizer tokenizer = new StringTokenizer(text);

        while (tokenizer.hasMoreTokens()) {
            String wordText = tokenizer.nextToken();

            // Detecting and excluding stop words should be done here
            //if (stopwords.contains(word)) continue;

            word.set(wordText);
            context.write(word, page);

        }

    }

    private boolean notValidPage(String pageString) {
        return pageString.contains(":");
    }

    private String[] parseTitleAndText(Text value) throws CharacterCodingException {
        String[] titleAndText = new String[2];

        int start = value.find("<title>");
        int end = value.find("</title>", start);
        start += 7; //add <title> length.

        titleAndText[0] = Text.decode(value.getBytes(), start, end-start);

        start = value.find("<text");
        start = value.find(">", start);
        end = value.find("</text>", start);
        start += 1;

        if(start == -1 || end == -1) {
            return new String[]{"",""};
        }

        titleAndText[1] = Text.decode(value.getBytes(), start, end-start);

        return titleAndText;
    }

}