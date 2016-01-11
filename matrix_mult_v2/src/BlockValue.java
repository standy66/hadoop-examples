import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by astepanov on 11/01/16.
 */
class BlockValue implements Writable {
    private LongWritable inBlockRowIndex;
    private LongWritable inBlockColumnIndex;
    private DoubleWritable value;
    private IntWritable matrixNumber;

    public BlockValue() {
        inBlockColumnIndex = new LongWritable();
        inBlockRowIndex = new LongWritable();
        value = new DoubleWritable();
        matrixNumber = new IntWritable();
    }

    public int getMatrixNumber() {
        return matrixNumber.get();
    }

    public LongWritable getInBlockRowIndex() {
        return inBlockRowIndex;
    }

    public LongWritable getInBlockColumnIndex() {
        return inBlockColumnIndex;
    }

    public double getValue() {
        return value.get();
    }

    public BlockValue(long newInBlockRowIndex, long newInBlockColumnIndex, double newValue, int newMatrixNumber) {
        this();
        inBlockRowIndex = new LongWritable(newInBlockRowIndex);
        inBlockColumnIndex = new LongWritable(newInBlockColumnIndex);
        value = new DoubleWritable(newValue);
        matrixNumber = new IntWritable(newMatrixNumber);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        inBlockRowIndex.write(dataOutput);
        inBlockColumnIndex.write(dataOutput);
        value.write(dataOutput);
        matrixNumber.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        inBlockRowIndex.readFields(dataInput);
        inBlockColumnIndex.readFields(dataInput);
        value.readFields(dataInput);
        matrixNumber.readFields(dataInput);
    }
}
