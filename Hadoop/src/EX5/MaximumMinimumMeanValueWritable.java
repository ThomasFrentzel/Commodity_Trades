package TDE.EX5;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MaximumMinimumMeanValueWritable implements WritableComparable<MaximumMinimumMeanValueWritable> {

    private double valorMax;
    private double valorMin;
    private double somaValores;
    private int qtd;

    public MaximumMinimumMeanValueWritable() {

    }

    public MaximumMinimumMeanValueWritable(double valorMax, double valorMin, double somaValores, int qtd) {
        this.valorMax = valorMax;
        this.valorMin = valorMin;
        this.somaValores = somaValores;
        this.qtd = qtd;
    }


    public double getValorMax() {
        return valorMax;
    }

    public void setValorMax(double valorMax) {
        this.valorMax = valorMax;
    }

    public double getValorMin() {
        return valorMin;
    }

    public void setValorMin(double valorMin) {
        this.valorMin = valorMin;
    }

    public double getSomaValores() {
        return somaValores;
    }

    public void setSomaValores(double somaValores) {
        this.somaValores = somaValores;
    }

    public int getQtd() {
        return qtd;
    }

    public void setQtd(int qtd) {
        this.qtd = qtd;
    }

    @Override
    public int compareTo(MaximumMinimumMeanValueWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(valorMax);
        dataOutput.writeDouble(valorMin);
        dataOutput.writeDouble(somaValores);
        dataOutput.writeInt(qtd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        valorMax = dataInput.readDouble();
        valorMin = dataInput.readDouble();
        somaValores = dataInput.readDouble();
        qtd = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MaximumMinimumMeanValueWritable that = (MaximumMinimumMeanValueWritable) o;
        return Double.compare(that.valorMax, valorMax) == 0 && Double.compare(that.valorMin, valorMin) == 0 && Double.compare(that.somaValores, somaValores) == 0 && qtd == that.qtd;
    }

    @Override
    public int hashCode() {
        return Objects.hash(valorMax, valorMin, somaValores, qtd);
    }
}
