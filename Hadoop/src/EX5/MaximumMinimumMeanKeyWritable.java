package TDE.EX5;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MaximumMinimumMeanKeyWritable implements WritableComparable<MaximumMinimumMeanKeyWritable> {

    private String ano;
    private String unitType;

    public MaximumMinimumMeanKeyWritable() {

    }

    public MaximumMinimumMeanKeyWritable(String ano, String unitType) {
        this.ano = ano;
        this.unitType = unitType;
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

    @Override
    public int compareTo(MaximumMinimumMeanKeyWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(ano);
        dataOutput.writeUTF(unitType);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ano = dataInput.readUTF();
        unitType = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MaximumMinimumMeanKeyWritable that = (MaximumMinimumMeanKeyWritable) o;
        return Objects.equals(ano, that.ano) && Objects.equals(unitType, that.unitType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ano, unitType);
    }

    @Override
    public String toString() {
        return ano + " - " + unitType + " - ";
    }
}
