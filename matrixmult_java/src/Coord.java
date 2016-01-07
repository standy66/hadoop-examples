import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by andrew on 07.01.16.
 */
public class Coord implements WritableComparable<Coord> {
    private LongWritable x, y;

    public Coord() {
        this(0, 0);
    }

    public Coord(long x, long y) {
        this.x = new LongWritable(x);
        this.y = new LongWritable(y);
    }

    @Override
    public int compareTo(Coord o) {
        if (x.compareTo(o.x) == 0) {
            return y.compareTo(o.y);
        }
        return x.compareTo(o.x);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        x.write(dataOutput);
        y.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        x.readFields(dataInput);
        y.readFields(dataInput);
    }

    public long getX() {
        return x.get();
    }

    public long getY() {
        return y.get();
    }

    public void setX(long x) {
        this.x.set(x);
    }

    public void setY(long y) {
        this.y.set(y);
    }

    @Override
    public int hashCode() {
        return x.hashCode() ^ y.hashCode();
    }

    @Override
    public String toString() {
        return String.format("(%d, %d)", x.get(), y.get());
    }
}
