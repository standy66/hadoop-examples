import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by astepanov on 11/01/16.
 */
class BlockReducer extends Reducer<BlockKey, BlockValue, MatrixCoords, DoubleWritable > {
    @Override
    protected void reduce(BlockKey key, Iterable<BlockValue> values, Context context) throws IOException, InterruptedException {
        int blockSize = (int) Long.parseLong(context.getConfiguration().get(MatrixMultJob.BLOCK_SIZE_CONF_KEY));
        long rowOffsetA = key.getRowBlockIndex() * blockSize;
        long columnOffsetB = key.getNumberOfCopy() * blockSize;

        double[][] blockA = new double[blockSize][blockSize];
        double[][] blockB = new double[blockSize][blockSize];

        for (BlockValue value : values) {
            int i = (int) value.getInBlockRowIndex().get();
            int j = (int) value.getInBlockColumnIndex().get();
            if (value.getMatrixNumber() == 0) {
                blockA[i][j] = value.getValue();
            } else {
                blockB[i][j] = value.getValue();
            }
        }

        for (int i = 0; i < blockSize; ++i) {
            for (int j = 0; j < blockSize; ++j) {
                double sum = 0;
                for (int k = 0; k < blockSize; ++k) {
                    sum += blockA[i][k] * blockB[k][j];
                }
                context.write(new MatrixCoords(rowOffsetA + i, columnOffsetB + j), new DoubleWritable(sum));
            }
        }
    }
}
