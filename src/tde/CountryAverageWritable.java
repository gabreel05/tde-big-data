package tde;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CountryAverageWritable implements WritableComparable<CountryAverageWritable> {
    private Double commoditySum;
    private Integer n;

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    private String country;

    public CountryAverageWritable(Double commoditySum, Integer n, String country) {
        this.commoditySum = commoditySum;
        this.n = n;
        this.country = country;
    }

    public CountryAverageWritable() {
    }

    public Double getCommoditySum() {
        return commoditySum;
    }

    public void setCommoditySum(Double commoditySum) {
        this.commoditySum = commoditySum;
    }

    public Integer getN() {
        return n;
    }

    public void setN(Integer n) {
        this.n = n;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CountryAverageWritable that = (CountryAverageWritable) o;
        return Objects.equals(commoditySum, that.commoditySum) && Objects.equals(n, that.n);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commoditySum, n);
    }

    @Override
    public int compareTo(CountryAverageWritable o) {
        return Integer.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(this.commoditySum);
        dataOutput.writeInt(this.n);
        dataOutput.writeUTF(this.country);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.commoditySum = dataInput.readDouble();
        this.n = dataInput.readInt();
        this.country = dataInput.readUTF();
    }
}
