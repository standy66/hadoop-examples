import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by astepanov on 11/01/16.
 */
class SumMapper extends Mapper<Object, Text, MatrixCoords, DoubleWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String stringValue = value.toString();
        String[] parsedValues = stringValue.split("\\s+");
        long row = Long.parseLong(parsedValues[0]);
        long column = Long.parseLong(parsedValues[1]);
        double matrixValue = Double.parseDouble(parsedValues[2]);
        context.write(new MatrixCoords(row, column), new DoubleWritable(matrixValue));
    }
}
