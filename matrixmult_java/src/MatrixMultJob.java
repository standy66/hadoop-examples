import com.google.common.hash.HashCode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Created by andrew on 07.01.16.
 */
public class MatrixMultJob {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("key.value.separator.in.input.line", "\t");
        configuration.setLong("matrixmult.interm_size", Long.parseLong(args[2]));
        Job job = Job.getInstance(configuration, "matirxmult");

        job.setJarByClass(MatrixMultJob.class);

        job.setMapperClass(MatrixMultMapper.class);
        job.setReducerClass(MatrixMultReducer.class);

        job.setPartitionerClass(HashPartitioner.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Coord.class);
        job.setOutputValueClass(RowColElem.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        job.waitForCompletion(false);
    }
}
