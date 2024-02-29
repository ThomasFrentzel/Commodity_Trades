package TDE.EX5;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MaximumMinimumMeanValue2Writable implements WritableComparable<MaximumMinimumMeanValue2Writable> {

    private double max;
    private double min;
    private double media;

    public MaximumMinimumMeanValue2Writable() {

    }

    public MaximumMinimumMeanValue2Writable(double max, double min, double media) {
        this.max = max;
        this.min = min;
        this.media = media;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMedia() {
        return media;
    }

    public void setMedia(double media) {
        this.media = media;
    }

    @Override
    public int compareTo(MaximumMinimumMeanValue2Writable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(max);
        dataOutput.writeDouble(min);
        dataOutput.writeDouble(media);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        max = dataInput.readDouble();
        min = dataInput.readDouble();
        media = dataInput.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MaximumMinimumMeanValue2Writable that = (MaximumMinimumMeanValue2Writable) o;
        return Double.compare(that.max, max) == 0 && Double.compare(that.min, min) == 0 && Double.compare(that.media, media) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(max, min, media);
    }

    @Override
    public String toString() {
        return "Max = " + max + " - " + "MIN = " + min + " - " + "Media = " + media;
    }
}
