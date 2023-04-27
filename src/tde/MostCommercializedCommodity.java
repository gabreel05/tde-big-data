package tde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.HashMap;

public class MostCommercializedCommodity {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "most commercialized commodity");
        job1.setJarByClass(MostCommercializedCommodity.class);
        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        Path input = new Path("in/transactions.csv");
        Path output = new Path("output/Intermediary.tmp");
        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, output);

        boolean success = job1.waitForCompletion(true);
        if (success) {
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "flow quantity");
            job2.setJarByClass(MostCommercializedCommodity.class);
            job2.setMapperClass(Mapper2.class);
            job2.setReducerClass(Reducer2.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            Path flowInput = new Path("output/Intermediary.tmp");
            Path flowOutput = new Path("output/Final.txt");
            FileInputFormat.addInputPath(job2, flowInput);
            FileOutputFormat.setOutputPath(job2, flowOutput);

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {



            // Ignore header row
            if (value.toString().startsWith("country")) {
                return;
            }

            String[] fields = value.toString().split(";");

            // Extract fields
            String commodity = fields[2];
            String year = fields[1];
            String flow = fields[4];
            Double quantity = Double.parseDouble(fields[8]);

            // Emit output for 2016 only
            if (year.equals("2016")) {
                context.write(new Text(commodity + ":" + flow), new DoubleWritable(quantity));
            }
        }
    }

    public static class Reducer1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            double sum = 0;

            for (DoubleWritable value : values) {
                sum += value.get();
            }

            context.write(key, new DoubleWritable(sum));
        }
    }

    public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(":");

            String commodity = fields[0];
            String flow = fields[1];
            double quantity = Double.parseDouble(value.toString().split("\t")[1]);

            context.write(new Text(flow), new Text(commodity + ":" + String.valueOf(quantity)));
        }
    }

    public static class Reducer2 extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            double maxQuantity = 0;
            String maxCommodity = "";

            for (Text value : values) {
                String[] fields = value.toString().split(":");
                String commodity = fields[0];
                double quantity = Double.parseDouble(fields[1]);

                if (quantity > maxQuantity) {
                    maxQuantity = quantity;
                    maxCommodity = commodity;
                }
            }


            context.write(key, new Text(maxCommodity + "," + maxQuantity));
        }
    }
}
