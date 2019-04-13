package edu.macalester.hadoop;

import org.apache.log4j.Logger;

import java.io.IOException;

public class InvertedIndex {

    private static final transient Logger logger = Logger.getLogger("app");


    public static void main(String[] args) {
        logger.debug("main");
        try {
            run(args[0], args[1]);
        } catch (IOException io) {
            logger.fatal("IO Exception " + InvertedIndex.class.getName() + " " + io);
        }

    }

    public static void run(String input, String output) throws IOException {

    }
}
