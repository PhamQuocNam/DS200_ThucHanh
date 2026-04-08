import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;
import java.util.*;

public class bt5 {

    // ════════════════════════════════════════════════════════════════
    // JOB 1: Đếm số lần xuất hiện của mỗi (category#word)
    // ════════════════════════════════════════════════════════════════

    public static class WordMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable one = new IntWritable(1);
        private final Text outputKey = new Text();
        private final Set<String> stopwords = new HashSet<String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String stopwordsPath = conf.get("stopwords.path", "/input/stopwords.txt");
            FileSystem fs = FileSystem.get(conf);
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(fs.open(new Path(stopwordsPath)), "UTF-8"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim().toLowerCase();
                    if (!line.isEmpty()) stopwords.add(line);
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("id;")) return;

            String[] parts = line.split(";", 5);
            if (parts.length < 5) return;

            String comment  = parts[1].toLowerCase();
            String category = parts[3].trim(); // entity

            String[] words = comment
                    .replaceAll("[^\\p{L}0-9 ]", " ")
                    .split("\\s+");

            for (String w : words) {
                w = w.trim();
                if (!w.isEmpty() && w.length() > 1 && !stopwords.contains(w)) {
                    outputKey.set(category + "#" + w);
                    context.write(outputKey, one);
                }
            }
        }
    }

    public static class SumReducer
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

    // ════════════════════════════════════════════════════════════════
    // JOB 2: Lấy Top 5 từ cho mỗi category
    // ════════════════════════════════════════════════════════════════

    public static class Top5Mapper
            extends Mapper<Object, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] tab = line.split("\t");
            if (tab.length < 2) return;

            String[] keyParts = tab[0].split("#", 2);
            if (keyParts.length < 2) return;

            String category = keyParts[0];
            String word     = keyParts[1];
            String count    = tab[1];

            outKey.set(category);
            outVal.set(word + "=" + count); // "phòng=6730"
            context.write(outKey, outVal);
        }
    }

    public static class Top5Reducer
            extends Reducer<Text, Text, Text, Text> {

        private final Text outVal = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // ✅ Dùng String[] {word, count} thay vì Map<Integer,String>
            //    để tránh mất từ khi 2 từ có cùng count
            PriorityQueue<String[]> pq = new PriorityQueue<String[]>(
                6, new Comparator<String[]>() {
                    @Override
                    public int compare(String[] a, String[] b) {
                        // min-heap theo count (index 1)
                        return Integer.parseInt(a[1]) - Integer.parseInt(b[1]);
                    }
                }
            );

            for (Text val : values) {
                String[] parts = val.toString().split("=", 2);
                if (parts.length < 2) continue;

                String word  = parts[0];
                String count = parts[1];

                pq.offer(new String[]{word, count});
                if (pq.size() > 5) pq.poll(); // loại phần tử nhỏ nhất
            }

            // Sắp xếp giảm dần
            List<String[]> top5 = new ArrayList<String[]>(pq);
            Collections.sort(top5, new Comparator<String[]>() {
                @Override
                public int compare(String[] a, String[] b) {
                    return Integer.parseInt(b[1]) - Integer.parseInt(a[1]);
                }
            });

            StringBuilder sb = new StringBuilder();
            for (String[] entry : top5) {
                sb.append(entry[0]).append("=").append(entry[1]).append(", ");
            }

            outVal.set(sb.toString().replaceAll(", $", ""));
            context.write(key, outVal);
        }
    }

    // ════════════════════════════════════════════════════════════════
    // DRIVER
    // ════════════════════════════════════════════════════════════════
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: bt5 <input> <tmp_output> <final_output>");
            System.exit(1);
        }

        // ── JOB 1 ────────────────────────────────────────────────────
        Configuration conf1 = new Configuration();
        conf1.set("stopwords.path", "/input/stopwords.txt");

        Job job1 = Job.getInstance(conf1, "Word Count By Category");
        job1.setJarByClass(bt5.class);
        job1.setMapperClass(WordMapper.class);
        job1.setCombinerClass(SumReducer.class);
        job1.setReducerClass(SumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        if (!job1.waitForCompletion(true)) System.exit(1);

        // ── JOB 2 ────────────────────────────────────────────────────
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Top 5 Words By Category");
        job2.setJarByClass(bt5.class);
        job2.setMapperClass(Top5Mapper.class);
        job2.setReducerClass(Top5Reducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}