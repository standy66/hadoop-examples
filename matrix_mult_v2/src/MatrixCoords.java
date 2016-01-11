import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by astepanov on 11/01/16.
 */
class MatrixCoords implements WritableComparable<MatrixCoords> {
    private LongWritable row;
    private LongWritable column;

    public MatrixCoords() {
        row = new LongWritable(0);
        column = new LongWritable(0);
    }

    @Override
    public String toString() {
        return String.format("%d %d", row.get(), column.get());
    }

    public MatrixCoords(long newRow, long newColumn) {
        row = new LongWritable(newRow);
        column = new LongWritable(newColumn);
    }

    @Override
    public int compareTo(MatrixCoords o) {
        int rowResult = row.compareTo(o.row);
        int columnResult = column.compareTo(o.column);
        if (rowResult == 0) {
            return columnResult;
        } else {
            return rowResult;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        row.write(dataOutput);
        column.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        row.readFields(dataInput);
        column.readFields(dataInput);
    }
}
