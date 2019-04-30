package edu.macalester.hadoop;

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableFieldSchema;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryTableSchema;
import com.google.cloud.hadoop.io.bigquery.output.IndirectBigQueryOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.ArrayList;
import java.util.List;
//import org.apache.log4j.Logger;

public class InvertedIndex extends Configured implements Tool {

//    private static final transient Logger logger = Logger.getLogger(InvertedIndex.class);


    public static void main(String[] args) throws Exception{
//        logger.info("Customized logging running");
        System.exit(ToolRunner.run(new Configuration(), new InvertedIndex(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        if (args.length < 3) {
            System.err.println("Arguments: <file_system> <input_folder> <output_folder>");
            return 2;
        }

        // Get the individual parameters from the command line.
        String projectId = "comp-445-mapreduce";
        String outputQualifiedTableId = "1.1";
        String outputGcsPath = args[2];

        // Define the schema we will be using for the output BigQuery table.
        List<BigQueryTableFieldSchema> outputTableFieldSchema = new ArrayList<>();
        outputTableFieldSchema.add(new BigQueryTableFieldSchema().setName("KeyWord").setType("STRING"));
        outputTableFieldSchema.add(new BigQueryTableFieldSchema().setName("IndexResult").setType("STRING"));
        BigQueryTableSchema outputSchema = new BigQueryTableSchema().setFields(outputTableFieldSchema);

        // Set the job-level projectId.
        conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId);

        // Configure input and output.
//        BigQueryConfiguration.configureBigQueryInput(conf, inputQualifiedTableId);
        BigQueryOutputConfiguration.configure(
                conf,
                outputQualifiedTableId,
                outputSchema,
                outputGcsPath,
                BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
                TextOutputFormat.class
        );

        Job job = Job.getInstance(conf, "invertedindex");

        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setInputFormatClass(XmlInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(IndirectBigQueryOutputFormat.class);

        Path fsPath = new Path(args[0]);
        Path inputPath = new Path(fsPath, args[1]);
        Path outputPath = new Path(fsPath, args[2]);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = fsPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
