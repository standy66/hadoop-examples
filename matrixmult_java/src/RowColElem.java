import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by andrew on 07.01.16.
 */
public class RowColElem implements Writable {
    private LongWritable index;
    private DoubleWritable value;


    public RowColElem() {
        this(0, 0);
    }

    public RowColElem(long index, double value) {
        this.index = new LongWritable(index);
        this.value = new DoubleWritable(value);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        index.write(dataOutput);
        value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        index.readFields(dataInput);
        value.readFields(dataInput);
    }

    public long getIndex() {
        return index.get();
    }

    public double getValue() {
        return value.get();
    }

    public void setIndex(long index) {
        this.index.set(index);
    }

    public void setValue(double value) {
        this.value.set(value);
    }

    @Override
    public String toString() {
        return String.format("%d %f", index.get(), value.get());
    }
}
