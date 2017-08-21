package homework.hadoop.writables;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@AllArgsConstructor
@ToString
@FieldDefaults(level = AccessLevel.PRIVATE)
public class TempRequestDataWritable implements Writable {

    LongWritable totalBytes;
    LongWritable amountOfRequests;

    public TempRequestDataWritable() {
        totalBytes = new LongWritable(0);
        amountOfRequests = new LongWritable(0);
    }

    public TempRequestDataWritable setTotalBytes(long totalBytes) {
        this.totalBytes.set(totalBytes);
        return this;
    }

    public TempRequestDataWritable setAmountOfRequests(long amountOfRequests) {
        this.amountOfRequests.set(amountOfRequests);
        return this;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        totalBytes.write(out);
        amountOfRequests.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        totalBytes.readFields(in);
        amountOfRequests.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TempRequestDataWritable that = (TempRequestDataWritable) o;

        return totalBytes.equals(that.totalBytes)
                && amountOfRequests.equals(that.amountOfRequests);
    }

    @Override
    public int hashCode() {
        int result = totalBytes.hashCode();
        result = 31 * result + amountOfRequests.hashCode();
        return result;
    }
}
