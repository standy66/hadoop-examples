import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

    public static class CompositeRecord implements WritableComparable<CompositeRecord> {
        private int replicaCount;
        private HashSet<Text> uniqueWords;

        public CompositeRecord() {
            replicaCount = 0;
            uniqueWords = new HashSet<>();
        }

        @Override
        public int compareTo(CompositeRecord o) {
            return replicaCount - o.replicaCount;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            new IntWritable(replicaCount).write(dataOutput);
            new IntWritable(uniqueWords.size()).write(dataOutput);
            for (Text word: uniqueWords) {
                word.write(dataOutput);
            }
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            IntWritable temp = new IntWritable();
            temp.readFields(dataInput);
            replicaCount = temp.get();
            temp.readFields(dataInput);
            int size = temp.get();
            uniqueWords.clear();
            for (int i = 0; i < size; ++i) {
                Text t = new Text();
                t.readFields(dataInput);
                uniqueWords.add(t);
            }
        }

        public void clear() {
            replicaCount = 0;
            uniqueWords.clear();
        }

        public void incReplicaCount() {
            replicaCount++;
        }

        public void incReplicaCount(int by) {
            replicaCount += by;
        }

        public void addWord(Text t) {
            uniqueWords.add(t);
        }

        public void addWords(HashSet<Text> words) {
            uniqueWords.addAll(words);
        }

        public HashSet<Text> getUniqueWords() {
            return uniqueWords;
        }

        public int getReplicaCount() {
            return replicaCount;
        }

        public void setReplicaCount(int newReplicaCount) {
            this.replicaCount = newReplicaCount;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (Text t: uniqueWords) {
                sb.append(t.toString() + " ");
            }
            return String.format("replicas: %d, uniqueWords: %d, words: %s", replicaCount, uniqueWords.size(), sb.toString());
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, CompositeRecord> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private String filter(String word) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < word.length(); ++i) {
                char ch = word.charAt(i);
                if (Character.getType(ch) == Character.LOWERCASE_LETTER) {
                    sb.append(ch);
                }
            }
            return sb.toString().trim();
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            Text name = new Text(st.nextToken("\t"));
            CompositeRecord ans = new CompositeRecord();
            ans.incReplicaCount();

            while (st.hasMoreTokens()) {
                String word = st.nextToken(" ");
                word = filter(word.toLowerCase());
                if (!word.isEmpty()) {
                    ans.addWord(new Text(word));
                }
            }
            context.write(name, ans);
        }
    }

    public static class Reduce extends Reducer<Text, CompositeRecord, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<CompositeRecord> values, Context context) throws IOException, InterruptedException {
            CompositeRecord ans = new CompositeRecord();
            for (CompositeRecord record: values) {
                ans.addWords(record.getUniqueWords());
                ans.incReplicaCount(record.getReplicaCount());
            }
            context.write(key, new Text(ans.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "wordcount");

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CompositeRecord.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}