package tde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class CountryWithHighestAverage {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();

        Configuration configuration = new Configuration();

        Path input = new Path("in/transactions.csv");
        Path output = new Path("output/CountryWithHighestAverage.txt");

        Job job = Job.getInstance(configuration, "CountryWithHighestAverage");

        job.setJarByClass(CountryWithHighestAverage.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CountryAverageWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(false);
    }

    public static class Map
            extends Mapper<LongWritable, Text, Text, CountryAverageWritable> {
        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, Text, CountryAverageWritable>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            if (!line.startsWith("country")) {
                String[] values = line.split(";");

                double commodityPrice = Double.parseDouble(values[5]);


                String flowType = values[4];
                String country = values[0];

                if (flowType.equals("Export")) {
                    context.write(new Text("Global"),
                            new CountryAverageWritable(commodityPrice, 1, country));
                }
            }
        }
    }

    public static class Combine
            extends Reducer<Text, CountryAverageWritable, Text, CountryAverageWritable> {
        @Override
        protected void reduce(Text key, Iterable<CountryAverageWritable> values,
                              Reducer<Text, CountryAverageWritable, Text, CountryAverageWritable>.Context context)
                throws IOException, InterruptedException {

            HashMap<String, Double> countryCommoditySum = new HashMap<>();
            HashMap<String, Integer> countryQuantitySum = new HashMap<>();

            for (CountryAverageWritable value : values) {
                String country = value.getCountry();
                double commoditySum = value.getCommoditySum();
                int quantitySum = value.getN();

                if (countryCommoditySum.containsKey(country)) {
                    countryCommoditySum.put(country, countryCommoditySum.get(country) + commoditySum);
                } else {
                    countryCommoditySum.put(country, commoditySum);
                }

                if (countryQuantitySum.containsKey(country)) {
                    countryQuantitySum.put(country, countryQuantitySum.get(country) + quantitySum);
                } else {
                    countryQuantitySum.put(country, quantitySum);
                }
            }

            for (String country : countryCommoditySum.keySet()) {
                context.write(key, new CountryAverageWritable(countryCommoditySum.get(country),
                        countryQuantitySum.get(country), country));
            }
        }
    }
    public static class Reduce
            extends Reducer<Text, CountryAverageWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<CountryAverageWritable> values,
                              Reducer<Text, CountryAverageWritable, Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException {

            HashMap<String, Double> countryCommoditySum = new HashMap<>();
            HashMap<String, Integer> countryQuantitySum = new HashMap<>();

            for (CountryAverageWritable value : values) {
                String country = value.getCountry();
                double commoditySum = value.getCommoditySum();
                int quantitySum = value.getN();

                if (countryCommoditySum.containsKey(country)) {
                    countryCommoditySum.put(country, countryCommoditySum.get(country) + commoditySum);
                } else {
                    countryCommoditySum.put(country, commoditySum);
                }

                if (countryQuantitySum.containsKey(country)) {
                    countryQuantitySum.put(country, countryQuantitySum.get(country) + quantitySum);
                } else {
                    countryQuantitySum.put(country, quantitySum);
                }
            }
            float highestAverage = 0;
            String highestAverageCountry = "";
            for (String country : countryCommoditySum.keySet()) {
                double commoditySum = countryCommoditySum.get(country);
                int quantitySum = countryQuantitySum.get(country);

                double average = commoditySum / quantitySum;

                if (average > highestAverage) {
                    highestAverage = (float) average;
                    highestAverageCountry = country;
                }
            }

            context.write(new Text(highestAverageCountry), new DoubleWritable(highestAverage));

        }
    }
}
