import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by andrew on 07.01.16.
 */
public class MatrixMultMapper extends Mapper<Text, Text, Coord, RowColElem> {
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        boolean isFirstMatrix = fileName.contains("A");
        long intermSize = context.getConfiguration().getLong("matrixmult.interm_size", -1);
        if (intermSize == -1) {
            throw new InterruptedException("Invalid configuration, should contain field intermSize");
        }
        System.out.println(key.toString() + value.toString());
        String[] splits = key.toString().split("\\s+");
        double elem = Double.parseDouble(value.toString());
        long x = Long.parseLong(splits[0]);
        long y = Long.parseLong(splits[1]);
        for (long i = 0; i < intermSize; ++i) {
            if (isFirstMatrix) {
                context.write(new Coord(x, i), new RowColElem(y, elem));
            } else {
                context.write(new Coord(i, y), new RowColElem(x, elem));
            }
        }
    }
}
