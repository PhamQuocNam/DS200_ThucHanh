import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class bt3 {

    // ── MAPPER ──────────────────────────────────────────────────────────────
    public static class SentimentMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable one = new IntWritable(1);
        private final Text pair = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("id;")) return;

            String[] parts = line.split(";", 5);
            if (parts.length < 5) return;

            String aspect    = parts[2].trim();
            String sentiment = parts[4].trim().toLowerCase();

            if (!aspect.isEmpty() &&
                    (sentiment.equals("positive") || sentiment.equals("negative"))) {
                pair.set(aspect + "#" + sentiment);
                context.write(pair, one);
            }
        }
    }

    // ── COMBINER (chỉ cộng tổng, KHÔNG có logic tìm max) ───────────────────
    public static class SumCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();
            result.set(sum);
            context.write(key, result);
        }
    }

    // ── REDUCER (cộng tổng + tìm max trong cleanup) ─────────────────────────
    public static class SumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private String maxPositiveAspect = "";
        private int    maxPositiveCount  = 0;
        private String maxNegativeAspect = "";
        private int    maxNegativeCount  = 0;

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) sum += val.get();

            context.write(key, new IntWritable(sum));

            String[] parts    = key.toString().split("#");
            String aspect     = parts[0];
            String sentiment  = parts[1];

            if (sentiment.equals("positive") && sum > maxPositiveCount) {
                maxPositiveCount  = sum;
                maxPositiveAspect = aspect;
            }
            if (sentiment.equals("negative") && sum > maxNegativeCount) {
                maxNegativeCount  = sum;
                maxNegativeAspect = aspect;
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            context.write(
                new Text("MOST POSITIVE ASPECT: " + maxPositiveAspect),
                new IntWritable(maxPositiveCount)
            );
            context.write(
                new Text("MOST NEGATIVE ASPECT: " + maxNegativeAspect),
                new IntWritable(maxNegativeCount)
            );
        }
    }

    // ── DRIVER ───────────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Aspect Sentiment Count");

        job.setJarByClass(bt3.class);
        job.setMapperClass(SentimentMapper.class);
        job.setCombinerClass(SumCombiner.class); 
        job.setReducerClass(SumReducer.class);
        job.setNumReduceTasks(1);                

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}