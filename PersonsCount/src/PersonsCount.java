import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.log4j.Logger;

public class PersonsCount extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PersonsCount.class);
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new PersonsCount(), args);
        System.exit(res);
    }
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "personscount");
        job.setJarByClass(this.getClass());
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(PersonsCountMapper.class);
        job.setCombinerClass(PersonsCountCombiner.class);
        job.setReducerClass(PersonsCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static class PersonsCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text nconst = new Text();
        private Text category = new Text();
        private final static IntWritable value = new IntWritable(1);

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            try {
                if (offset.get() == 0)
                    return;
                else {
                    String line = lineText.toString();
                    if (line.length() < 5)
                        return;

                    int i = 0;
                    for (String word : line.split("\t")) {
                        if (i == 2) {
                            nconst.set(word);
                        }
                        if (i == 3) {
                            if (word.equals("director")) {
                                category.set("director");
                            }
                            else if (word.equals("actor") || word.equals("actress") || word.equals("self")) {
                                category.set("actor");
                            }
                            else
                                return;
                        }
                        i++;
                    }
                    String key = nconst.toString()+"\t"+category.toString();
                    nconst.set(key);
                    context.write(nconst, value);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public static class PersonsCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum =0;
            for (IntWritable val : values) {
                sum+=val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class PersonsCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum =0;
            for (IntWritable val : values) {
                sum+=val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
