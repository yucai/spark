package org.apache.spark.network.shuffle.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.Encoders;

import java.util.Arrays;

public class HierarchyLayerInfo implements Encodable {
    public final String key;
    public final long threshold;
    public final String[] dirs;

    public HierarchyLayerInfo(String key, Long threshold, String[] dirs) {
        this.key = key;
        this.threshold = threshold;
        this.dirs = dirs;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key, threshold) * 41 + Arrays.hashCode(dirs);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("key", key)
                .add("threshold", threshold)
                .add("dirs", Arrays.toString(dirs))
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof HierarchyLayerInfo) {
            HierarchyLayerInfo o = (HierarchyLayerInfo) other;
            return Objects.equal(key, o.key)
                    && Objects.equal(threshold, o.threshold)
                    && Arrays.equals(dirs, o.dirs);
        }
        return false;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(key)
                + 8 // long
                + Encoders.StringArrays.encodedLength(dirs);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, key);
        buf.writeLong(threshold);
        Encoders.StringArrays.encode(buf, dirs);
    }

    public static HierarchyLayerInfo decode(ByteBuf buf) {
        String key = Encoders.Strings.decode(buf);
        long threshold = buf.readLong();
        String[] dirs = Encoders.StringArrays.decode(buf);
        return new HierarchyLayerInfo(key, threshold, dirs);
    }
}