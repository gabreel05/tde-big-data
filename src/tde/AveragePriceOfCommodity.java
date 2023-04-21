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

public class AveragePriceOfCommodity {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();

        Configuration configuration = new Configuration();

        Path input = new Path("in/transactions.csv");
        Path output = new Path("/output/AveragePriceOfCommodity.txt");

        Job job = Job.getInstance(configuration, "AveragePriceOfCommodity");

        job.setJarByClass(AveragePriceOfCommodity.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(AveragePriceOfCommodityKeyWritable.class);
        job.setMapOutputValueClass(AveragePriceOfCommodityValueWritable.class);
        job.setOutputKeyClass(AveragePriceOfCommodityKeyWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(false);
    }

    public static class Map
            extends Mapper<LongWritable, Text, AveragePriceOfCommodityKeyWritable, AveragePriceOfCommodityValueWritable> {
        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, AveragePriceOfCommodityKeyWritable, AveragePriceOfCommodityValueWritable>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            if (line.startsWith("country")) {
                String[] values = line.split("/");

                double commodityPrice = Double.parseDouble(values[5]);

                String unitType = values[7];
                int year = Integer.parseInt(values[1]);
                String category = values[9];

                String flowType = values[5];
                String country = values[0];

                if (flowType.equals("Export") && country.equals("Brazil")) {
                    context.write(new AveragePriceOfCommodityKeyWritable(unitType, year, category),
                            new AveragePriceOfCommodityValueWritable(commodityPrice, 1));
                }
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
            extends Reducer<AveragePriceOfCommodityKeyWritable, AveragePriceOfCommodityValueWritable, AveragePriceOfCommodityKeyWritable, DoubleWritable> {
        @Override
        protected void reduce(AveragePriceOfCommodityKeyWritable key, Iterable<AveragePriceOfCommodityValueWritable> values,
                              Reducer<AveragePriceOfCommodityKeyWritable, AveragePriceOfCommodityValueWritable, AveragePriceOfCommodityKeyWritable, DoubleWritable>.Context context)
                throws IOException, InterruptedException {
            double commoditySum = 0;
            int quantitySum = 0;

            for (AveragePriceOfCommodityValueWritable value : values) {
                commoditySum += value.getCommoditySum();
                quantitySum += value.getN();
            }

            double average = commoditySum / quantitySum;

            context.write(key, new DoubleWritable(average));
        }
    }
}
