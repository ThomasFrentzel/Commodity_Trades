package TDE.EX4;

import TDE.EX3.CommodValueWritable;
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import org.checkerframework.checker.units.qual.A;

import java.io.IOException;

public class AverageCommodities {


    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "averageCommodity");

        // Registrar as classes
        j.setJarByClass(AverageCommodities.class);
        j.setMapperClass(MapForAverageCommodities.class);
        j.setReducerClass(ReduceForAverageCommodities.class);

        j.setCombinerClass(CombineForAverageCommodities.class);

        // Definir os tipos de saida
        // MAP
        j.setMapOutputKeyClass(AverageCommodityKeyWritable.class);
        j.setMapOutputValueClass(AverageCommodityValueWritable.class);

        // REDUCE
        j.setOutputKeyClass(AverageCommodityKeyWritable.class);
        j.setOutputValueClass(DoubleWritable.class);

        // Definir arquivos de entrada e de saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // rodar :)
        j.waitForCompletion(false);
    }

    public static class MapForAverageCommodities extends Mapper<LongWritable, Text, AverageCommodityKeyWritable, AverageCommodityValueWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // obtendo a linha
            String linha = value.toString();

            // ignorando o cabeçalho
            if (!linha.startsWith("country_or_area;")) {

                // quebrando em colunas
                String colunas[] = linha.split(";");

                // obtendo as chaves
                String pais = colunas[0];
                String flow = colunas[4];

                // verificando se o país é Brazil
                if (pais.equals("Brazil")) {

                    // verificando se o flow é Export
                    if (flow.equals("Export")) {

                        // chaves
                        String ano = colunas[1];
                        String unitType = colunas[7];
                        String categoria = colunas[9];

                        // valores
                        double valor = Double.parseDouble(colunas[5]);
                        int qtd = 1;

                        // chave, valor
                        AverageCommodityKeyWritable chaves = new AverageCommodityKeyWritable(ano, unitType, categoria);
                        AverageCommodityValueWritable valores = new AverageCommodityValueWritable(valor, qtd);

                        con.write(chaves, valores);
                    }
                }
                }
            }
        }

    public static class CombineForAverageCommodities extends Reducer<AverageCommodityKeyWritable, AverageCommodityValueWritable, AverageCommodityKeyWritable, AverageCommodityValueWritable> {

        public void reduce(AverageCommodityKeyWritable key, Iterable<AverageCommodityValueWritable> values, Context con)
                throws IOException, InterruptedException {

            // somar os valores e as qtds
            double somaVals = 0.0;
            int somaQtds = 0;
            for(AverageCommodityValueWritable o : values){
                somaVals += o.getSomaValores();
                somaQtds += o.getQtd();
            }

            // passando para o reduce valores pre-somados
            con.write(key, new AverageCommodityValueWritable(somaVals, somaQtds));
        }
    }
    public static class ReduceForAverageCommodities extends Reducer<AverageCommodityKeyWritable, AverageCommodityValueWritable, AverageCommodityKeyWritable, DoubleWritable> {

        private int count = 0;

        public void reduce(AverageCommodityKeyWritable key, Iterable<AverageCommodityValueWritable> values, Context con)
                throws IOException, InterruptedException {

            // somar os valores e as qtds
            double somaVals = 0.0;
            int somaQtds = 0;
            for (AverageCommodityValueWritable o : values){
                somaVals += o.getSomaValores();
                somaQtds += o.getQtd();
            }

            // calcular a media
            double media = somaVals / somaQtds;

            // para manter 5 linhas apenas no output
            if (count <= 4) {
                con.write(key, new DoubleWritable(media));
                count++;
            }
        }
    }
}
