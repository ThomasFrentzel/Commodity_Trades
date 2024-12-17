package TDE.EX5;


import TDE.EX3.CommodValueWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.Arrays;

public class MaximumMinimumMean {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path(files[0]);

        Path output = new Path(files[1]);

        Job j = new Job(c, "MaximumMinimumMean");

        j.setJarByClass(MaximumMinimumMean.class);
        j.setMapperClass(MapForMaximumMinimumMean.class);
        j.setReducerClass(ReduceForMaximumMinimumMean.class);
        j.setCombinerClass(CombineForMaximumMinimumMean.class);

        j.setMapOutputKeyClass(MaximumMinimumMeanKeyWritable.class);
        j.setMapOutputValueClass(MaximumMinimumMeanValueWritable.class);

        j.setOutputKeyClass(MaximumMinimumMeanKeyWritable.class);
        j.setOutputValueClass(MaximumMinimumMeanValue2Writable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);
    }

    public static class MapForMaximumMinimumMean extends Mapper<LongWritable, Text, MaximumMinimumMeanKeyWritable, MaximumMinimumMeanValueWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            if (!linha.startsWith("country_or_area;")) {

                String colunas[] = linha.split(";");

                String ano = colunas[1];
                String unitType = colunas[7];

                double valorMax = Double.parseDouble(colunas[5]);
                double valorMin = Double.parseDouble(colunas[5]);
                double valor = Double.parseDouble(colunas[5]);
                int qtd = 1;

                MaximumMinimumMeanKeyWritable chaves = new MaximumMinimumMeanKeyWritable(ano, unitType);
                MaximumMinimumMeanValueWritable valores = new MaximumMinimumMeanValueWritable(valorMax, valorMin, valor, qtd);

                con.write(chaves, valores);
            }
        }
    }
    public static class CombineForMaximumMinimumMean extends Reducer<MaximumMinimumMeanKeyWritable, MaximumMinimumMeanValueWritable, MaximumMinimumMeanKeyWritable, MaximumMinimumMeanValueWritable>{

        public void reduce(MaximumMinimumMeanKeyWritable key, Iterable<MaximumMinimumMeanValueWritable> values, Context con)
                throws IOException, InterruptedException {

            double max = 0.0;
            double min = 10000;
            double somaVals = 0;
            int somaQtds = 0;


            for (MaximumMinimumMeanValueWritable j : values) {
                somaVals += j.getSomaValores();
                somaQtds += j.getQtd();
                if (j.getValorMax() > max) {
                    max = j.getValorMax();
                }

                if (j.getValorMin() < min) {
                    min = j.getValorMin();
                }
            }

            con.write(key, new MaximumMinimumMeanValueWritable(max, min, somaVals, somaQtds));

        }
    }
    public static class ReduceForMaximumMinimumMean extends Reducer<MaximumMinimumMeanKeyWritable, MaximumMinimumMeanValueWritable, MaximumMinimumMeanKeyWritable, MaximumMinimumMeanValue2Writable> {

        private int count = 0;

        public void reduce(MaximumMinimumMeanKeyWritable key, Iterable<MaximumMinimumMeanValueWritable> values, Context con)
                throws IOException, InterruptedException {

            double max = 0.0;
            double min = 10000;
            double somaVals = 0;
            int somaQtds = 0;

            for (MaximumMinimumMeanValueWritable j : values) {
                somaVals += j.getSomaValores();
                somaQtds += j.getQtd();
                if (j.getValorMax() > max) {
                    max = j.getValorMax();
                }

                if (j.getValorMin() < min) {
                    min = j.getValorMin();
                }
            }

            double media = somaVals / somaQtds;

            if (count <= 4) {
                con.write(key, new MaximumMinimumMeanValue2Writable(max, min, media));
                count++;
            }

        }
    }
}
