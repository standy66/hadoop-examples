import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by astepanov on 11/01/16.
 */
class BlockMapper extends Mapper<Object,Text, BlockKey, BlockValue> {

    @Override
    protected void map(Object key, Text line, Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        long I = Long.parseLong(configuration.get(MatrixMultJob.SIZE_1_CONF_KEY));
        long K = Long.parseLong(configuration.get(MatrixMultJob.SIZE_2_CONF_KEY));
        long J = Long.parseLong(configuration.get(MatrixMultJob.SIZE_3_CONF_KEY));
        long blockSize = Long.parseLong(configuration.get(MatrixMultJob.BLOCK_SIZE_CONF_KEY));
        int matrixNo = ((FileSplit) context.getInputSplit()).getPath().getName().contains("B") ? 1 : 0;

        long blockColumns = matrixNo == 0 ? J / blockSize : K / blockSize;
        long blockRows = matrixNo == 0 ? K / blockSize : I / blockSize;

        String[] words = line.toString().split("\\s+");
        long rowIndex = Long.parseLong(words[0]);
        long columnIndex = Long.parseLong(words[1]);
        double value = Double.parseDouble(words[2]);

        if (matrixNo == 0) {
            for (long i = 0; i < blockColumns; ++i) {
                context.write(new BlockKey(rowIndex / blockSize, columnIndex / blockSize, i),
                              new BlockValue(rowIndex % blockSize, columnIndex % blockSize, value, matrixNo));
            }
        } else {
            for (long i = 0; i < blockRows; ++i) {
                context.write(new BlockKey(i, rowIndex / blockSize, columnIndex / blockSize),
                              new BlockValue(rowIndex % blockSize, columnIndex % blockSize, value, matrixNo));
            }
        }
    }
}
