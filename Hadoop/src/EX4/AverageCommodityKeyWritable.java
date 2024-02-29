package TDE.EX4;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class AverageCommodityKeyWritable implements WritableComparable<AverageCommodityKeyWritable> {

    private String ano;
    private String unitType;
    private String categoria;

    public AverageCommodityKeyWritable() {

    }

    public AverageCommodityKeyWritable(String ano, String unitType, String categoria) {
        this.ano = ano;
        this.unitType = unitType;
        this.categoria = categoria;
    }

    public String getAno() {
        return ano;
    }

    public void setAno(String ano) {
        this.ano = ano;
    }

    public String getUnitType() {
        return unitType;
    }

    public void setUnitType(String unitType) {
        this.unitType = unitType;
    }

    public String getCategoria() {
        return categoria;
    }

    public void setCategoria(String categoria) {
        this.categoria = categoria;
    }

    @Override
    public int compareTo(AverageCommodityKeyWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(ano);
        dataOutput.writeUTF(unitType);
        dataOutput.writeUTF(categoria);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ano = dataInput.readUTF();
        unitType = dataInput.readUTF();
        categoria = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AverageCommodityKeyWritable that = (AverageCommodityKeyWritable) o;
        return Objects.equals(ano, that.ano) && Objects.equals(unitType, that.unitType) && Objects.equals(categoria, that.categoria);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ano, unitType, categoria);
    }

    @Override
    public String toString() {
        return ano + " - " + unitType + " - " + categoria + " - ";
    }

}
