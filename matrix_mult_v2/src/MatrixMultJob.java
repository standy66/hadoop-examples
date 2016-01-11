import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by astepanov on 11/01/16.
 */

public class MatrixMultJob {
    public static final String BLOCK_SIZE_CONF_KEY = "BLOCK_SIZE";
    public static final String SIZE_1_CONF_KEY = "SIZE_1";
    public static final String SIZE_2_CONF_KEY = "SIZE_2";
    public static final String SIZE_3_CONF_KEY = "SIZE_3";

    public static void main(String[] args) throws Exception {
        Configuration jobConf = new Configuration();
        jobConf.set(SIZE_1_CONF_KEY,args[2]);
        jobConf.set(SIZE_2_CONF_KEY, args[3]);
        jobConf.set(SIZE_3_CONF_KEY, args[4]);
        jobConf.set(BLOCK_SIZE_CONF_KEY, args[5]);

        Job multBlocksJob = Job.getInstance( jobConf, "blocks_multiplication");
        multBlocksJob.setJarByClass(MatrixMultJob.class);
        multBlocksJob.setMapperClass(BlockMapper.class);
        multBlocksJob.setReducerClass(BlockReducer.class);
        multBlocksJob.setInputFormatClass(TextInputFormat.class);
        multBlocksJob.setOutputFormatClass(TextOutputFormat.class);
        multBlocksJob.setOutputKeyClass(BlockKey.class);
        multBlocksJob.setOutputValueClass(BlockValue.class);
        FileInputFormat.addInputPath(multBlocksJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(multBlocksJob, new Path("tmp"));

        Job sumJob = Job.getInstance(new Configuration(), "blocks_summation");
        sumJob.setJarByClass(MatrixMultJob.class);
        sumJob.setMapperClass(SumMapper.class);
        sumJob.setReducerClass(SumReducer.class);
        sumJob.setInputFormatClass(TextInputFormat.class);
        sumJob.setOutputFormatClass(TextOutputFormat.class);
        sumJob.setOutputKeyClass(MatrixCoords.class);
        sumJob.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(sumJob, new Path("tmp"));
        FileOutputFormat.setOutputPath(sumJob, new Path(args[1]));

        multBlocksJob.waitForCompletion(false);
        sumJob.waitForCompletion(false);
    }

}