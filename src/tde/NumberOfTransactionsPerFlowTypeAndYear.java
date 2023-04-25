package tde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class NumberOfTransactionsPerFlowTypeAndYear {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();

        Configuration configuration = new Configuration();

        Path input = new Path("in/transactions.csv");
        Path output = new Path("output/NumberOfTransactionsPerFlowTypeAndYear.txt");

        Job job = Job.getInstance(configuration, "NumberOfTransactionsPerFlowTypeAndYear");

        job.setJarByClass(NumberOfTransactionsPerFlowTypeAndYear.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(NumberOfTransactionsPerFlowTypeAndYearWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NumberOfTransactionsPerFlowTypeAndYearWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(false);
    }

    public static class Map
            extends Mapper<LongWritable, Text, NumberOfTransactionsPerFlowTypeAndYearWritable, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, NumberOfTransactionsPerFlowTypeAndYearWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            if (!line.startsWith("country_or_area")) {
                String[] values = line.split(";");

                String flowType = values[4];
                int year = Integer.parseInt(values[1]);

                context.write(new NumberOfTransactionsPerFlowTypeAndYearWritable(flowType, year), new IntWritable(1));
            }
        }
    }

    public static class Reduce
            extends Reducer<NumberOfTransactionsPerFlowTypeAndYearWritable, IntWritable, NumberOfTransactionsPerFlowTypeAndYearWritable, IntWritable> {
        @Override
        protected void reduce(NumberOfTransactionsPerFlowTypeAndYearWritable key, Iterable<IntWritable> values,
                              Reducer<NumberOfTransactionsPerFlowTypeAndYearWritable, IntWritable, NumberOfTransactionsPerFlowTypeAndYearWritable, IntWritable>.Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }
}
