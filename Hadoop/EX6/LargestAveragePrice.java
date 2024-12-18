package TDE.EX6;

import TDE.EX5.MaximumMinimumMeanKeyWritable;
import TDE.EX5.MaximumMinimumMeanValueWritable;
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

public class LargestAveragePrice {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);

        Path intermediate = new Path("./output/ex6.tmp");

        Path output = new Path(files[1]);

        Job j1 = new Job(c, "average1");
        j1.setJarByClass(LargestAveragePrice.class);
        j1.setMapperClass(MapEtapaA.class);
        j1.setReducerClass(ReduceEtapaA.class);
        j1.setCombinerClass(CombineEtapaA.class);
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(LargestAveragePriceValueWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        j1.waitForCompletion(false);

        Job j2 = new Job(c, "average2");
        j2.setJarByClass(LargestAveragePrice.class);
        j2.setMapperClass(MapEtapaB.class);
        j2.setReducerClass(ReduceEtapaB.class);
        j2.setCombinerClass(CombineEtapaB.class);
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(LargestAveragePriceValue2Writable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(LargestAveragePriceValue2Writable.class);

        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);


        j2.waitForCompletion(false);


    }

    public static class MapEtapaA extends Mapper<LongWritable, Text, Text, LargestAveragePriceValueWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            if (!linha.startsWith("country_or_area;")) {

                String colunas[] = linha.split(";");

                String flow = colunas[4];

                if (flow.equals("Export")) {

                    String pais = colunas[0];

                    double valor = Double.parseDouble(colunas[5]);
                    int qtd = 1;

                    LargestAveragePriceValueWritable valores = new LargestAveragePriceValueWritable(valor, qtd);

                    con.write(new Text(pais), valores);
                }
                }
            }
        }

    public static class CombineEtapaA extends Reducer<Text, LargestAveragePriceValueWritable, Text, LargestAveragePriceValueWritable>{

        public void reduce(Text key, Iterable<LargestAveragePriceValueWritable> values, Context con)
                throws IOException, InterruptedException {

            double somaVals = 0.0;
            int somaQtds = 0;

            for (LargestAveragePriceValueWritable o : values) {
                somaVals += o.getSomaValores();
                somaQtds += o.getQtd();
            }

            con.write(key, new LargestAveragePriceValueWritable(somaVals, somaQtds));

        }
    }

    public static class ReduceEtapaA extends Reducer<Text, LargestAveragePriceValueWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<LargestAveragePriceValueWritable> values, Context con)
                throws IOException, InterruptedException {

            double somaVals = 0.0;
            int somaQtds = 0;

            for (LargestAveragePriceValueWritable o : values) {
                somaVals += o.getSomaValores();
                somaQtds += o.getQtd();
            }

            double media = somaVals / somaQtds;

            con.write(key, new DoubleWritable(media));
        }
    }


    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, LargestAveragePriceValue2Writable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            String linhas[] = linha.split("\t");

            String pais = linhas[0];
            double qtd = Double.parseDouble(linhas[1]);

            Text chave = new Text("País com maior média: ");

            LargestAveragePriceValue2Writable valor = new LargestAveragePriceValue2Writable(pais, qtd);

            con.write(chave, valor);

        }
    }

    public static class CombineEtapaB extends Reducer<Text, LargestAveragePriceValue2Writable, Text, LargestAveragePriceValue2Writable> {
        public void reduce(Text key, Iterable<LargestAveragePriceValue2Writable> values, Context con)
                throws IOException, InterruptedException {

            double largest = 0.0;
            String pais = "";

            for (LargestAveragePriceValue2Writable o : values) {
                if (o.getQtd() > largest) {
                    largest = o.getQtd();
                    pais = o.getPais();
                }
            }

            Text chave = new Text(key);
            LargestAveragePriceValue2Writable valores = new LargestAveragePriceValue2Writable(pais, largest);

            con.write(chave, valores);
        }
    }


    public static class ReduceEtapaB extends Reducer<Text, LargestAveragePriceValue2Writable, Text, LargestAveragePriceValue2Writable> {
        public void reduce(Text key, Iterable<LargestAveragePriceValue2Writable> values, Context con)
                throws IOException, InterruptedException {

            double largest = 0.0;
            String pais = "";


            for (LargestAveragePriceValue2Writable o : values) {
                if (o.getQtd() > largest) {
                    largest = o.getQtd();
                    pais = o.getPais();
                }
            }


            Text chave = new Text(key);
            LargestAveragePriceValue2Writable valores = new LargestAveragePriceValue2Writable(pais, largest);

            con.write(chave, valores);
        }
    }
}
