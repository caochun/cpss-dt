package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.exception.DuplicateTimestampException;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;

import java.util.ArrayList;

public class StatsBlockLeaf extends StatsBlock{
    public static final byte BLOCK_TYPE = 2;

    /** size of stat header without statistics:
     *  block type(1B) + prevRID(12B) + nextRID(12B)
     */
    public static final int HEADER_WITHOUT_STATS = 25;

    public StatsBlock parent;
    public ArrayList<DataPoint> dataList;
    public RID prevRID;
    public RID succRID;
    // for unfixed data type
    public int dataBytesUseed;

    public StatsBlockLeaf(ArcadeDocumentManager manager, Document document, String metric, int degree, DataType dataType, long startTime, boolean isLatest) throws TimeseriesException {
        super(manager, document, metric, degree, dataType, startTime, isLatest);
        statistics = Statistics.newEmptyStats(dataType);
    }

    void loadData() throws TimeseriesException {
        if (dataList == null) {
            dataList = new ArrayList<>();
            Binary binary = new Binary(document.getBinary("data"));
            for (int i = 0; i < statistics.count; i++)
                dataList.add(DataPoint.getDataPointFromBinary(dataType, binary));
            if (!dataType.isFixed()){
                dataBytesUseed = 0;
                for (DataPoint dataPoint : dataList)
                    dataBytesUseed += dataPoint.realBytesRequired();
            }
        }
    }

    @Override
    public MutableDocument serializeDocument() throws TimeseriesException {
        int statSize = HEADER_WITHOUT_STATS + Statistics.maxBytesRequired(dataType);

        MutableDocument mutableDocument = document.modify();
        // put stat
        Binary statBinary = new Binary(statSize, false);
        statBinary.putByte(BLOCK_TYPE);
        statistics.serialize(statBinary);
        statBinary.putInt(prevRID.getBucketId());
        statBinary.putLong(prevRID.getPosition());
        statBinary.putInt(succRID.getBucketId());
        statBinary.putLong(succRID.getPosition());

        if (statBinary.size() > statSize)
            throw new TimeseriesException("stat header size exceeded");

        statBinary.size(statSize);
        mutableDocument.set("stat", statBinary.toByteArray());

        // put data
        if (dataList != null){
            Binary dataBinary = new Binary(MAX_DATA_BLOCK_SIZE, false);
            for (int i=0; i<statistics.count; i++){
                dataList.get(i).serialize(dataBinary);
            }

            if (dataBinary.size() > MAX_DATA_BLOCK_SIZE)
                throw new TimeseriesException("data block size exceeded");

            dataBinary.size(MAX_DATA_BLOCK_SIZE);
            mutableDocument.set("data", dataBinary.toByteArray());
        }
        return mutableDocument;
    }

    @Override
    public void setParent(StatsBlock parent) {
        this.parent = parent;
    }

    @Override
    public void insert(DataPoint data, boolean updateIfExist) throws TimeseriesException {
        if (dataType.isFixed())
            insertFixed(data, updateIfExist);
        else{
            insertUnfixed(data, updateIfExist);
        }
    }

    public void insertFixed(DataPoint data, boolean updateIfExist) throws TimeseriesException {
        if (data.timestamp < startTime)
            throw new TimeseriesException("target dataPoint should not be handled by this leaf");

        if (dataList == null)
            loadData();

        int maxdataSize = MAX_DATA_BLOCK_SIZE / DataPoint.maxBytesRequired(dataType);

        if (isLatest && dataList.size() >= maxdataSize)
            throw new TimeseriesException("latest leaf block is full to insert, which should not occur");

        boolean update = false;
        int pos = -1;
        if (dataList.size() == 0 || dataList.get(0).timestamp > data.timestamp){
            // insert at head
            pos = 0;
        }else if (dataList.get(dataList.size()-1).timestamp < data.timestamp){
            // insert at tail
            pos = dataList.size();
        }else {
            // binary search
            int low = 0, high = dataList.size() - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                long midTime = dataList.get(mid).timestamp;
                if (midTime < data.timestamp)
                    low = mid + 1;
                else if (midTime > data.timestamp)
                    high = mid - 1;
                else {
                    if (!updateIfExist)
                        throw new DuplicateTimestampException("datapoint already exist in metric '"+ metric +"' at timestamp "+dataList.get(mid).timestamp);
                    // update
                    update = true;
                    pos = mid;
                    break;
                }
            }
            if (!update)
                pos = low;
        }

        if (update){
            DataPoint oldDP = dataList.get(pos);
            // old and new have same value
            if (oldDP.getValue().equals(data.getValue()))
                return;
            dataList.set(pos, data);
            if (!statistics.update(oldDP, data)){
                statistics = Statistics.countStats(dataType, dataList, true);
            }
            if (!isLatest)
                parent.updateStats(oldDP, data);
        }else {
            dataList.add(pos, data);
            statistics.insert(data);
            if (!isLatest)
                parent.appendStats(data);
        }

        if (isLatest){
            // commit latest block if full
            if (dataList.size() == maxdataSize){
                parent.appendStats(this.statistics);

                DataPoint lastDataPoint = dataList.get(maxdataSize - 1);
                StatsBlockLeaf newLeaf = (StatsBlockLeaf) manager.newArcadeDocument(PREFIX_STATSBLOCK+ metric, document1 -> {
                    return new StatsBlockLeaf(manager, document1, metric, degree, dataType, lastDataPoint.timestamp+1, true);
                });
                this.isLatest = false;

                newLeaf.dataList = new ArrayList<>();

                // link leaves
                newLeaf.prevRID = this.document.getIdentity();
                newLeaf.succRID = manager.nullRID;
                newLeaf.save();
                this.succRID = newLeaf.document.getIdentity();

                parent.addChild(newLeaf);
            }
        }else{
            // split if full
            if (dataList.size() > maxdataSize){
                // split into 2 blocks
                int totalSize = dataList.size();
                int splitedSize = totalSize / 2;
                DataPoint LatterfirstDataPoint = dataList.get(splitedSize);

                StatsBlockLeaf newLeaf = (StatsBlockLeaf) manager.newArcadeDocument(PREFIX_STATSBLOCK+ metric, document1 -> {
                    return new StatsBlockLeaf(manager, document1, metric, degree, dataType, LatterfirstDataPoint.timestamp, false);
                });
                newLeaf.dataList = new ArrayList<>(this.dataList.subList(splitedSize, totalSize));

                // calc latter half statistics
                newLeaf.statistics = Statistics.countStats(dataType, newLeaf.dataList, true);

                // update this block statistics
                this.dataList = new ArrayList<>(this.dataList.subList(0, splitedSize));
                this.statistics = Statistics.countStats(dataType, this.dataList, true);

                // link leaves
                StatsBlockLeaf succLeaf = (StatsBlockLeaf) getStatsBlockNonRoot(manager, this.succRID, null, metric, degree, dataType, -1, false);
                newLeaf.succRID = this.succRID;
                newLeaf.prevRID = this.document.getIdentity();
                newLeaf.save();
                this.succRID = newLeaf.document.getIdentity();
                succLeaf.prevRID = newLeaf.document.getIdentity();
                succLeaf.setAsDirty();

                parent.addChild(newLeaf);
            }
        }
        setAsDirty();
    }

    private void insertUnfixed(DataPoint data, boolean updateIfExist) throws TimeseriesException {
        if (data.timestamp < startTime)
            throw new TimeseriesException("target dataPoint should not be handled by this leaf");

        if (dataList == null)
            loadData();

        boolean update = false;
        int pos = -1;
        if (dataList.size() == 0 || dataList.get(0).timestamp > data.timestamp){
            // insert at head
            pos = 0;
        }else if (dataList.get(dataList.size()-1).timestamp < data.timestamp){
            // insert at tail
            pos = dataList.size();
        }else {
            // binary search
            int low = 0, high = dataList.size() - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                long midTime = dataList.get(mid).timestamp;
                if (midTime < data.timestamp)
                    low = mid + 1;
                else if (midTime > data.timestamp)
                    high = mid - 1;
                else {
                    if (!updateIfExist)
                        throw new DuplicateTimestampException("datapoint already exist in metric '"+ metric +"' at timestamp "+dataList.get(mid).timestamp);
                    // update
                    update = true;
                    pos = mid;
                    break;
                }
            }
            if (!update)
                pos = low;
        }

        if (update){
            DataPoint oldDP = dataList.get(pos);
            // old and new have same value
            if (oldDP.getValue().equals(data.getValue()))
                return;
            dataList.set(pos, data);
            dataBytesUseed += data.realBytesRequired() - oldDP.realBytesRequired();
            if (!statistics.update(oldDP, data)){
                statistics = Statistics.countStats(dataType, dataList, true);
            }
            if (!isLatest)
                parent.updateStats(oldDP, data);
        }else {
            dataList.add(pos, data);
            dataBytesUseed += data.realBytesRequired();
            statistics.insert(data);
            if (!isLatest)
                parent.appendStats(data);
        }

        // check if require split, to achieve split, each data point should use at most MAX_DATA_BLOCK_SIZE/2 bytes
        if (dataBytesUseed > MAX_DATA_BLOCK_SIZE){
            // locate split size
            int splitedBytes, splitedSize;
            if (isLatest){
                // fill this block as much as possible
                splitedBytes = dataBytesUseed;
                splitedSize = dataList.size();
                while (splitedBytes > MAX_DATA_BLOCK_SIZE){
                    splitedSize--;
                    splitedBytes -= dataList.get(splitedSize).realBytesRequired();
                }
            }else{
                // try to split evenly.
                splitedBytes = 0;
                splitedSize = 0;
                int halfBytes = dataBytesUseed / 2;

                while (splitedBytes < halfBytes){
                    splitedBytes += dataList.get(splitedSize).realBytesRequired();
                    splitedSize++;
                }

                // one step back if better
                int stepBackBytes = splitedBytes - dataList.get(splitedSize - 1).realBytesRequired();
                if (splitedBytes > MAX_DATA_BLOCK_SIZE || splitedBytes - halfBytes > halfBytes - stepBackBytes){
                    splitedBytes = stepBackBytes;
                    splitedSize--;
                }
            }

            // create latter half leaf node
            DataPoint LatterfirstDataPoint = dataList.get(splitedSize);
            StatsBlockLeaf newLeaf = (StatsBlockLeaf) manager.newArcadeDocument(PREFIX_STATSBLOCK+ metric, document1 -> {
                return new StatsBlockLeaf(manager, document1, metric, degree, dataType, LatterfirstDataPoint.timestamp, isLatest);
            });
            newLeaf.dataList = new ArrayList<>(this.dataList.subList(splitedSize, dataList.size()));
            newLeaf.dataBytesUseed = this.dataBytesUseed - splitedBytes;
            newLeaf.statistics = Statistics.countStats(dataType, newLeaf.dataList, true);

            // re-calc stats
            this.dataBytesUseed = splitedBytes;
            this.dataList = new ArrayList<>(this.dataList.subList(0, splitedSize));
            statistics = Statistics.countStats(dataType, dataList, true);

            // link leaves
            if (isLatest) {
                // commit statistics if is latest
                parent.appendStats(this.statistics);

                newLeaf.prevRID = this.document.getIdentity();
                newLeaf.succRID = manager.nullRID;
                newLeaf.save();
                this.succRID = newLeaf.document.getIdentity();
            }else{
                StatsBlockLeaf succLeaf = (StatsBlockLeaf) getStatsBlockNonRoot(manager, this.succRID, null, metric, degree, dataType, -1, false);
                newLeaf.succRID = this.succRID;
                newLeaf.prevRID = this.document.getIdentity();
                newLeaf.save();
                this.succRID = newLeaf.document.getIdentity();
                succLeaf.prevRID = newLeaf.document.getIdentity();
                succLeaf.setAsDirty();
            }

            parent.addChild(newLeaf);
        }
        setAsDirty();
    }

    @Override
    public void appendStats(DataPoint data) throws TimeseriesException {
        throw new TimeseriesException("leaf node should not append statistics");
    }

    @Override
    public void appendStats(Statistics stats) throws TimeseriesException {
        throw new TimeseriesException("leaf node should not append statistics");
    }

    @Override
    public void updateStats(DataPoint oldDP, DataPoint newDP) throws TimeseriesException {
        throw new TimeseriesException("leaf node should not update statistics");
    }

    @Override
    public void addChild(StatsBlock child) throws TimeseriesException {
        throw new TimeseriesException("cannot add child to leaf node");
    }

    @Override
    public Statistics aggregativeQuery(long startTime, long endTime) throws TimeseriesException {
        // if range out of this block
        if (startTime > statistics.lastTime || endTime < statistics.firstTime){
            return Statistics.newEmptyStats(dataType);
        }

        // if range covers this block
        if (startTime <= statistics.firstTime && endTime >= statistics.lastTime){
            return this.statistics.clone();
        }

        if (dataList == null)
            loadData();

        // locate first DataPoint
        int startPos = -1;
        if (statistics.firstTime >= startTime){
            // start from head
            startPos = 0;
        }else {
            // binary search
            int low = 0, high = dataList.size() - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                long midTime = dataList.get(mid).timestamp;
                if (midTime > startTime)
                    high = mid - 1;
                else if (midTime < startTime)
                    low = mid + 1;
                else {
                    startPos = mid;
                    break;
                }
            }
            if (low > high)
                startPos = low;
        }

        // locate last DataPoint
        int endPos = -1;
        if (statistics.lastTime <= endTime){
            endPos = dataList.size()-1;
        }else{
            // binary search
            int low = 0, high = dataList.size() - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                long midTime = dataList.get(mid).timestamp;
                if (midTime > endTime)
                    high = mid - 1;
                else if (midTime < endTime)
                    low = mid + 1;
                else {
                    endPos = mid;
                    break;
                }
            }
            if (low > high)
                endPos = high;
        }

        return Statistics.countStats(dataType, dataList.subList(startPos, endPos+1), true);
    }

    @Override
    public DataPointSet periodQuery(long startTime, long endTime) throws TimeseriesException {
        if (startTime < this.startTime)
            throw new TimeseriesException("period query over-headed");

        return new DataPointSet(startTime, endTime, this.document.getIdentity(), manager, metric, degree, dataType);
    }
}
