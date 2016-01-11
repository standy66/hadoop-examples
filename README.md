# hadoop-examples
Toying with Hadoop MapReduce.

There are 4 tasks in here:
- A variation of word counting using Java
- Another variation of word counting using Hadoop Streaming and Python 3
- Matrix multiplication on Java
- Enhanced (block) matrix multiplication on Java

### Matrix multiplication

Each element of input matrices A or B, namely (i, j) -> Aval or (i, j) -> Bval, is mapped into array [(i, k) -> (Aval, j) | 0 <= k < n] or [(k, j) -> (Bval, i) | 0 <= k < n]. After grouping on single reducer we have a row and a column that we have to multiply.

The most significant disadvantage of this method is network traffic - while multiplying matrices with sizes 1000x3000, 3000x2000 mapping stage yields about 30 GB of data.

### Enhanced matrix multiplication

Input matrices are divided into square blocks. There are two jobs in this solution. First job multiplies this square blocks, while another job sums them. First mapper maps (i, j) -> A[i, j] to [(i / blockSize, j / blockSize, k) -> (A[i, j], i % blockSize, j % blockSize, k) | 0 <= k < n / blockSize]. This formula looks almost identically for matrix B. Then, first reducers have 2 square blocks that it has to multiply. Second mapper is identity. Then, second reducers just sums the blocks it has for input.

I measured performance of this approach on matrices with the following sizes: *500x1000*, *1000x2000*. Single-threaded python solution took around *5 minutes* to complete. While MapReduce solution took 56 s for block multiplying job and 50 s for summing job on 4 node dev cluster, resulting in *1 minute 46 seconds* total.
