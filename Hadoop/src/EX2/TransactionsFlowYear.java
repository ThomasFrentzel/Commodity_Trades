package TDE.EX2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


public class TransactionsFlowYear {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "flow_year");

        // Registro de Classes
        j.setJarByClass(TransactionsFlowYear.class); // classe que contem o método MAIN
        j.setMapperClass(MapForFlowYear.class); // classe que contem o método MAP
        j.setReducerClass(ReduceForFlowYear.class); // classe que contem o método REDUCE

        j.setCombinerClass(CombineForFlowYear.class); // classe que contem o método COMBINER

        // Definir os tipos de saída
        j.setMapOutputKeyClass(FlowYearWritable.class); // tipo de chave de saída do MAP
        j.setMapOutputValueClass(IntWritable.class); // tipo do valor de saída do MAP

        j.setOutputKeyClass(FlowYearWritable.class); // tipo de chave de saida do REDUCE
        j.setOutputValueClass(IntWritable.class); // tipo de valor de saída do REDUCE

        // Definindo arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Executando a rotina
        j.waitForCompletion(false);

    }

    public static class MapForFlowYear extends Mapper<LongWritable, Text, FlowYearWritable, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Covertendo a linha de entrada em uma string
            String linha = value.toString();

            // ignorando o cabeçalho
            if (!linha.startsWith("country_or_area;")) {

                /// quebrando em colunas
                String colunas[] = linha.split(";");

                // pegando as chaves
                String ano = colunas[1];
                String flow = colunas[4];

                // chave, valor
                FlowYearWritable keys = new FlowYearWritable(ano, flow);
                IntWritable valor = new IntWritable(1);

                con.write(keys, valor);
            }

        }
    }

    public static class CombineForFlowYear extends Reducer<FlowYearWritable, IntWritable, FlowYearWritable, IntWritable> {

        private int count = 0;

        public void reduce(FlowYearWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int cont = 0;

            // contagem
            for (IntWritable v : values) {
                cont += v.get();
            }

            // para manter 5 linhas apenas no output
            if (count <= 4) {
                con.write(key, new IntWritable(cont));
                count++;
            }
        }
    }

    public static class ReduceForFlowYear extends Reducer<FlowYearWritable, IntWritable, FlowYearWritable, IntWritable> {

        private int count = 0;

        public void reduce(FlowYearWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int cont = 0;

            // contagem
            for (IntWritable v : values) {
                cont += v.get();
            }

            // para manter 5 linhas apenas no output
            if (count <= 4) {
                con.write(key, new IntWritable(cont));
                count++;
            }
        }
    }


}
