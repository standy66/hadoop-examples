import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by astepanov on 11/01/16.
 */
class BlockKey implements WritableComparable<BlockKey> {
    private LongWritable rowBlockIndex;
    private LongWritable columnBlockIndex;
    private LongWritable numberOfCopy;

    public BlockKey() {
        rowBlockIndex = new LongWritable(0);
        columnBlockIndex = new LongWritable(0);
        numberOfCopy = new LongWritable(0);
    }

    public BlockKey(long newRowBlockIndex, long newColumnBlockIndex, long newNumberOfCopy) {
        rowBlockIndex = new LongWritable(newRowBlockIndex);
        columnBlockIndex = new LongWritable(newColumnBlockIndex);
        numberOfCopy = new LongWritable(newNumberOfCopy);
    }

    public long getRowBlockIndex() {
        return rowBlockIndex.get();
    }

    public long getNumberOfCopy() {
        return numberOfCopy.get();
    }

    @Override
    public int compareTo(BlockKey o) {
        int rowIndexResult = rowBlockIndex.compareTo(o.rowBlockIndex);
        int columnIndexResult = columnBlockIndex.compareTo(o.columnBlockIndex);
        int copyResult = numberOfCopy.compareTo(o.numberOfCopy);
        if (rowIndexResult == 0) {
            if (columnIndexResult == 0) {
                return copyResult;
            } else {
                return columnIndexResult;
            }
        } else {
            return rowIndexResult;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        rowBlockIndex.write(dataOutput);
        columnBlockIndex.write(dataOutput);
        numberOfCopy.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        rowBlockIndex.readFields(dataInput);
        columnBlockIndex.readFields(dataInput);
        numberOfCopy.readFields(dataInput);
    }
}
