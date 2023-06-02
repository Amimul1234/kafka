package org.example.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class SensorPartitioner implements Partitioner {

    private String speedSensorName;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numOfPartitions = partitions.size();
        int sp = (int) Math.abs(numOfPartitions * 0.3);
        int p;

        if (keyBytes == null || (!(key instanceof String))) {
            throw new InvalidRecordException("All message must have sensor name as key");
        }

        if (key.equals(speedSensorName)) {
            p = Utils.toPositive(Utils.murmur2(valueBytes)) % sp;
        } else {
            p = Utils.toPositive(Utils.murmur2(keyBytes)) % (numOfPartitions - sp) + sp;
        }

        return p;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        speedSensorName = configs.get("speed.sensor.name").toString();
    }

}
