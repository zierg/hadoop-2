package homework.hadoop.writables;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class RequestDataWritable implements Writable {

    FloatWritable averageBytes;
    LongWritable totalBytes;

    public RequestDataWritable() {
        averageBytes = new FloatWritable(0);
        totalBytes = new LongWritable(0);
    }

    public RequestDataWritable setAverageBytes(float averageBytes) {
        this.averageBytes.set(averageBytes);
        return this;
    }

    public RequestDataWritable setTotalBytes(long totalBytes) {
        this.totalBytes.set(totalBytes);
        return this;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        averageBytes.write(out);
        totalBytes.write(out);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        averageBytes.readFields(in);
        totalBytes.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RequestDataWritable that = (RequestDataWritable) o;

        return averageBytes.equals(that.averageBytes) && totalBytes.equals(that.totalBytes);
    }

    @Override
    public int hashCode() {
        int result = averageBytes.hashCode();
        result = 31 * result + totalBytes.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return averageBytes + "," + totalBytes;
    }
}
