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

        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "MaximumMinimumMean");

        // Registrar as classes
        j.setJarByClass(MaximumMinimumMean.class);
        j.setMapperClass(MapForMaximumMinimumMean.class);
        j.setReducerClass(ReduceForMaximumMinimumMean.class);
        j.setCombinerClass(CombineForMaximumMinimumMean.class);

        // Definir os tipos de saida
        // MAP
        j.setMapOutputKeyClass(MaximumMinimumMeanKeyWritable.class);
        j.setMapOutputValueClass(MaximumMinimumMeanValueWritable.class);

        // REDUCE
        j.setOutputKeyClass(MaximumMinimumMeanKeyWritable.class);
        j.setOutputValueClass(MaximumMinimumMeanValue2Writable.class);

        // Definir arquivos de entrada e de saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // rodar :)
        j.waitForCompletion(false);
    }

    public static class MapForMaximumMinimumMean extends Mapper<LongWritable, Text, MaximumMinimumMeanKeyWritable, MaximumMinimumMeanValueWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // obtendo a linha
            String linha = value.toString();

            // ignorando o cabeçalho
            if (!linha.startsWith("country_or_area;")) {

                // quebrando em colunas
                String colunas[] = linha.split(";");

                // chave
                String ano = colunas[1];
                String unitType = colunas[7];

                // valor
                double valorMax = Double.parseDouble(colunas[5]);
                double valorMin = Double.parseDouble(colunas[5]);
                double valor = Double.parseDouble(colunas[5]);
                int qtd = 1;

                // chave, valor
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

            // somar os valores e as qtds
            // obter os valores máximos e mínimos
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

            // somar os valores e as qtds
            // obter os valores máximos e mínimos
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

            // calcular a media
            double media = somaVals / somaQtds;

            // para manter 5 linhas apenas no output
            if (count <= 4) {
                con.write(key, new MaximumMinimumMeanValue2Writable(max, min, media));
                count++;
            }

        }
    }
}
