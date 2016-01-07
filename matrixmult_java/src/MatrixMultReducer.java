import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by andrew on 07.01.16.
 */
public class MatrixMultReducer extends Reducer<Coord, RowColElem, Coord, DoubleWritable> {

    private HashMap<Long, Double> result = new HashMap<>();

    @Override
    protected void reduce(Coord key, Iterable<RowColElem> values, Context context) throws IOException, InterruptedException {
        result.clear();
        for (RowColElem rowColElem: values) {
            long idx = rowColElem.getIndex();
            double elem = rowColElem.getValue();
            Double current = result.get(idx);
            if (current == null) {
                result.put(idx, elem);
            } else {
                result.put(idx, elem * current);
            }
        }
        double sum = 0;
        for (double val: result.values()) {
            sum += val;
        }
        context.write(key, new DoubleWritable(sum));
    }
}
