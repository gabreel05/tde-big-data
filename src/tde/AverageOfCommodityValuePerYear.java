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

public class AverageOfCommodityValuePerYear {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();

        Configuration configuration = new Configuration();

        Path input = new Path("in/transactions.csv");
        Path output = new Path("/output/AverageOfCommodityValuePerYear.txt");

        Job job = Job.getInstance(configuration, "AverageOfCommodityValuePerYear");

        job.setJarByClass(AverageOfCommodityValuePerYear.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(AverageOfCommodityValuePerYearWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(false);
    }

    public static class Map
            extends Mapper<LongWritable, Text, IntWritable, AverageOfCommodityValuePerYearWritable> {
        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, IntWritable, AverageOfCommodityValuePerYearWritable>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            if (!line.startsWith("country")) {
                String[] values = line.split("/");

                double commodityValue = Double.parseDouble(values[5]);
                int year = Integer.parseInt(values[1]);

                context.write(new IntWritable(year), new AverageOfCommodityValuePerYearWritable(commodityValue, 1));
            }
        }
    }

    public static class Combine
            extends Reducer<IntWritable, AverageOfCommodityValuePerYearWritable, IntWritable, AverageOfCommodityValuePerYearWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<AverageOfCommodityValuePerYearWritable> values,
                              Reducer<IntWritable, AverageOfCommodityValuePerYearWritable, IntWritable, AverageOfCommodityValuePerYearWritable>.Context context)
                throws IOException, InterruptedException {
            double commoditySum = 0;
            int quantitySum = 0;

            for (AverageOfCommodityValuePerYearWritable value : values) {
                commoditySum += value.getCommoditySum();
                quantitySum += value.getN();
            }

            context.write(key, new AverageOfCommodityValuePerYearWritable(commoditySum, quantitySum));
        }
    }

    public static class Reduce
            extends Reducer<IntWritable, AverageOfCommodityValuePerYearWritable, IntWritable, DoubleWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<AverageOfCommodityValuePerYearWritable> values,
                              Reducer<IntWritable, AverageOfCommodityValuePerYearWritable, IntWritable, DoubleWritable>.Context context)
                throws IOException, InterruptedException {
            double commoditySum = 0;
            int quantitySum = 0;

            for (AverageOfCommodityValuePerYearWritable value : values) {
                commoditySum += value.getCommoditySum();
                quantitySum += value.getN();
            }

            double average = commoditySum / quantitySum;

            context.write(key, new DoubleWritable(average));
        }
    }
}
