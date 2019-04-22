package edu.macalester.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wikiclean.WikiClean;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.StringTokenizer;


public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final transient Logger logger = Logger.getLogger(InvertedIndexMapper.class);

    private Text word = new Text();
    private WikiClean cleaner = new WikiClean.Builder()
            .withFooter(false)
            .withLanguage(WikiClean.WikiLanguage.EN)
            .withTitle(false)
            .build();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] titleAndText;

        try {
            titleAndText = parseTitleAndText(value);
        } catch (IllegalArgumentException e) {
            //throw new IOException("Article not parsed.");
            return;
        }


        String pageString = titleAndText[0];
        String text = titleAndText[1].toLowerCase();

        if(notValidPage(pageString))
            return;

        Text page = new Text(pageString.replace(' ', '_'));
        StringTokenizer tokenizer = new StringTokenizer(text, " \t\n\r\f\",.:;?!#[](){}*");

        while (tokenizer.hasMoreTokens()) {
            String wordText = tokenizer.nextToken();
            word.set(wordText);
            context.write(word, page);
        }
    }

    private boolean notValidPage(String pageString) {
        return pageString.contains(":");
    }

    private String[] parseTitleAndText(Text value) {
        String[] titleAndText = new String[2];

        String valueStr = value.toString();

        titleAndText[0] = cleaner.getTitle(valueStr);

        try {
            titleAndText[1] = cleaner.clean(valueStr);
        } catch (IllegalArgumentException e) {
            logger.info("MY_LOGGING: Caught IllegalArgumentException in parsing text body.");
            logger.info("MY_LOGGING: Title: " + titleAndText[0]);
            throw e;
        }

        return titleAndText;
    }

}