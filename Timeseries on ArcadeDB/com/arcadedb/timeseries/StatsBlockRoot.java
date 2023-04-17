package com.arcadedb.timeseries;

import com.arcadedb.database.Binary;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;

import java.util.ArrayList;

public class StatsBlockRoot extends StatsBlock{
    public static final byte BLOCK_TYPE = 0;

    /**
     * size of stat header without statistics and child list:
     * block type(1B) + degree(4B) + data type(5B) + child size(4B)
     */
    public static final int HEADER_WITHOUT_STATS_AND_CHILD = 14;

    public ArrayList<RID> childRID = new ArrayList<>();
    public ArrayList<Long> childStartTime = new ArrayList<>();

    public StatsBlockRoot(ArcadeVertexManager manager, Vertex vertex, String measurement, int degree, DataType dataType) {
        // root is always latest and start at 0
        super(manager, vertex, measurement, degree, dataType, 0, true);
    }

    @Override
    public void setParent(StatsBlock parent) {
        // ignore
    }

    @Override
    public MutableVertex serializeVertex() throws TimeseriesException {
        int statSize = HEADER_WITHOUT_STATS_AND_CHILD + Statistics.bytesToWrite(dataType) + degree * CHILD_SIZE;

        MutableVertex modifiedVertex = vertex.modify();
        Binary binary = new Binary(statSize, false);
        binary.putByte(BLOCK_TYPE);
        binary.putInt(degree);
        dataType.serialize(binary);
        statistics.serialize(binary);
        binary.putInt(childRID.size());
        for (int i=0; i<childRID.size(); i++){
            binary.putInt(childRID.get(i).getBucketId());
            binary.putLong(childRID.get(i).getPosition());
            binary.putLong(childStartTime.get(i));
        }

        if (binary.size() > statSize)
            throw new TimeseriesException("stat header size exceeded");

        binary.size(statSize);
        modifiedVertex.set("stat", binary.toByteArray());

        return modifiedVertex;
    }

    @Override
    public boolean insert(DataPoint data) throws TimeseriesException {
        if (childStartTime.size() == 0)
            throw new TimeseriesException("cannot insert datapoint as there's no child block");

        int pos = -1;
        if (childStartTime.get(childRID.size()-1) <= data.timestamp){
            // insert into latest
            pos = childRID.size()-1;
        }else {
            // binary search
            int low = 0, high = childRID.size() - 1;
            // for not latest block
            while (low <= high) {
                int mid = (low + high) >>> 1;
                long midStartTime = childStartTime.get(mid);
                if (midStartTime > data.timestamp)
                    high = mid - 1;
                else if (midStartTime < data.timestamp)
                    low = mid + 1;
                else {
                    pos = mid;
                    break;
                }
            }
            if (low > high)
                pos = high;
        }

        boolean isInsertLatest = pos == childRID.size() - 1;
        return StatsBlock.getStatsBlockNonRoot(manager, childRID.get(pos), this, measurement, degree, dataType, childStartTime.get(pos), isInsertLatest).insert(data);
    }

    @Override
    public void appendStats(DataPoint data) throws TimeseriesException {
        this.statistics.insert(data);
        setAsDirty();
    }

    @Override
    public void appendStats(Statistics stats) throws TimeseriesException {
        this.statistics.merge(stats);
        setAsDirty();
    }

    @Override
    public void addChild(StatsBlock child) throws TimeseriesException {
        int pos = -1;
        if (childRID.size() == 0 || childStartTime.get(0) > child.startTime){
            // insert at head
            pos = 0;
        }else if (childStartTime.get(childRID.size()-1) < child.startTime){
            // insert at tail
            pos = childRID.size();
        }else {
            // binary search
            int low = 0, high = childRID.size() - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                long midStartTime = childStartTime.get(mid);
                if (midStartTime < child.startTime)
                    low = mid + 1;
                else if (midStartTime > child.startTime)
                    high = mid - 1;
                else
                    throw new TimeseriesException("cannot insert child with existing startTime");
            }
            pos = low;
        }

        childRID.add(pos, child.vertex.getIdentity());
        childStartTime.add(pos, child.startTime);

        if (childRID.size() == degree){
            StatsBlockInternal newInternal = (StatsBlockInternal) manager.newArcadeVertex(PREFIX_STATSBLOCK+measurement, vertex1 -> {
                return new StatsBlockInternal(manager, vertex1, measurement, degree, dataType, 0, true);
            });
            newInternal.childRID = this.childRID;
            newInternal.childStartTime = this.childStartTime;
            newInternal.statistics = this.statistics.clone();
            newInternal.save();

            this.childRID = new ArrayList<>();
            this.childStartTime = new ArrayList<>();
            this.childRID.add(newInternal.vertex.getIdentity());
            this.childStartTime.add(0L);
        }
        setAsDirty();
    }

    @Override
    public Statistics aggregativeQuery(long startTime, long endTime) throws TimeseriesException {
        // empty block
        if (childRID.size() == 0)
            throw new TimeseriesException("root has no child to calc statistics");


        int lastChildIndex = childRID.size() - 1;
        Statistics resultStats;
        if (endTime >= childStartTime.get(lastChildIndex)) {
            // calc latest child's statistics as it is out of current statistics
            resultStats = StatsBlock.getStatsBlockNonRoot(manager, childRID.get(lastChildIndex), this, measurement, degree, dataType, childStartTime.get(lastChildIndex), true)
                    .aggregativeQuery(startTime, endTime);
            lastChildIndex--;
        }else{
            resultStats = Statistics.newEmptyStats(dataType);
        }

        // if range out of root's statistics
        if (startTime > statistics.lastTime || endTime < statistics.firstTime){
            return resultStats;
        }
        // if range covers root's statistics
        if (startTime <= statistics.firstTime && endTime >= statistics.lastTime){
            resultStats.merge(this.statistics);
            return resultStats;
        }

        // locate first block
        int pos = -1;
        if (childStartTime.get(0) >= startTime){
            // start from head
            pos = 0;
        }else {
            // binary search
            int low = 0, high = childRID.size() - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                long midStartTime = childStartTime.get(mid);
                if (midStartTime > startTime)
                    high = mid - 1;
                else if (midStartTime < startTime)
                    low = mid + 1;
                else {
                    pos = mid;
                    break;
                }
            }
            if (low > high)
                pos = high;
        }

        // merge non-latest block's statistics
        while (pos <= lastChildIndex && childStartTime.get(pos) <= endTime){
            resultStats.merge(StatsBlock.getStatsBlockNonRoot(manager, childRID.get(pos), this, measurement, degree, dataType, childStartTime.get(pos), false).aggregativeQuery(startTime, endTime));
            pos++;
        }
        return resultStats;
    }

    @Override
    public DataPointSet periodQuery(long startTime, long endTime) throws TimeseriesException {
        // locate first block
        int pos = -1;
        if (childStartTime.get(0) >= startTime){
            // start from head
            pos = 0;
        }else {
            // binary search
            int low = 0, high = childRID.size() - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                long midStartTime = childStartTime.get(mid);
                if (midStartTime > startTime)
                    high = mid - 1;
                else if (midStartTime < startTime)
                    low = mid + 1;
                else {
                    pos = mid;
                    break;
                }
            }
            if (low > high)
                pos = high;
        }

        return StatsBlock.getStatsBlockNonRoot(manager, childRID.get(pos), this, measurement, degree, dataType, childStartTime.get(pos), pos == childRID.size()-1).periodQuery(startTime, endTime);
    }
}
