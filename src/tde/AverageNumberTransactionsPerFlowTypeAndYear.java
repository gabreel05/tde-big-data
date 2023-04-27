
package tde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AverageNumberTransactionsPerFlowTypeAndYear {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "number transactions per year and flow type");

        // Registro de classes
        j.setJarByClass(AverageNumberTransactionsPerFlowTypeAndYear.class); // Classe que tem o método MAIN
        j.setMapperClass(MapForCountTransactionKeys.class); // Classe do MAP
        j.setReducerClass(ReduceForCountTransactionKeys.class); // Classe do REDUCE
       // j.setCombinerClass(ReduceForCountTransactionKeys.class);

        // Tipos de saida
        j.setMapOutputKeyClass(TransactionWritable.class); // tipo da chave de saida do MAP
        j.setMapOutputValueClass(IntWritable.class); // tipo do valor de saida do MAP
        j.setOutputKeyClass(TransactionWritable.class); // tipo de chave de saida do reduce
        j.setOutputValueClass(DoubleWritable.class); // tipo de valor de saida do reduce

        // Definindo arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input); // adicionando o caminho do input no job
        FileOutputFormat.setOutputPath(j, output); // adicionando o caminho de output no job

        // rodar :)
        j.waitForCompletion(false);

    }

    public static class MapForCountTransactionKeys extends Mapper<LongWritable, Text,
            TransactionWritable, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!key.equals(new LongWritable(0))) {
                String line = value.toString();

                String[] words = line.split(";");

                String year = words[1];
                String unitType = words[7];

                TransactionWritable mapKey = new TransactionWritable(Integer.parseInt(year), unitType);
                IntWritable mapValue = new IntWritable(1);

                context.write(mapKey, mapValue);
            }

        }
    }

    public static class ReduceForCountTransactionKeys extends Reducer<TransactionWritable, IntWritable, TransactionWritable, DoubleWritable> {
        public void reduce(TransactionWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (IntWritable value : values) {
                sum += value.get();
                count = count + 1;
            }
            double average = (double) sum / count;
            context.write(key, new DoubleWritable(average));

        }
    }
}