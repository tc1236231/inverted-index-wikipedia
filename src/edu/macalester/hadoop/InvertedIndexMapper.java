package edu.macalester.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Pattern;


public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Pattern wikiLinksPattern = Pattern.compile("\\[.+?\\]");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // Returns  String[0] = <title>[TITLE]</title>
        //          String[1] = <text>[CONTENT]</text>
        // !! without the <tags>.
        String[] titleAndText = parseTitleAndText(value);

        String pageString = titleAndText[0];
        if(notValidPage(pageString))
            return;

        Text page = new Text(pageString.replace(' ', '_'));


    }

    private boolean notValidPage(String pageString) {
        return pageString.contains(":");
    }

    private String sweetify(String aLinkText) {
        if(aLinkText.contains("&amp;"))
            return aLinkText.replace("&amp;", "&");

        return aLinkText;
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