package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;

import java.util.ArrayList;

public abstract class StatsBlock extends ArcadeDocument{
    public static final int DEFAULT_TREE_DEGREE = 10;
    public static final int MAX_DATA_BLOCK_SIZE = 4096;
    public static final String PREFIX_STATSBLOCK = "_stat_";

    /**
     * size of one child block
     * childRID(12B) + childStartTime(8B)
     */
    public static final int CHILD_SIZE = 20;

    public final String metric;
    public final int degree;
    public final DataType dataType;
    public long startTime;
    public Statistics statistics;
    public boolean isLatest = false;

    public StatsBlock(ArcadeDocumentManager manager, Document document, String metric, int degree, DataType dataType, long startTime, boolean isLatest){
        super(manager, document);
        this.metric = metric;
        this.degree = degree;
        this.dataType = dataType;
        this.startTime = startTime;
        this.isLatest = isLatest;
    }

    public static StatsBlockRoot newStatsTree(ArcadeDocumentManager manager, String metric, DataType dataType) throws TimeseriesException {
        return newStatsTree(manager, metric, dataType, DEFAULT_TREE_DEGREE);
    }

    public static StatsBlockRoot newStatsTree(ArcadeDocumentManager manager, String metric, DataType dataType, int degree) throws TimeseriesException {
        // root node
        StatsBlockRoot newTreeRoot = (StatsBlockRoot) manager.newArcadeDocument(PREFIX_STATSBLOCK+metric, document1 -> {
            StatsBlockRoot root = new StatsBlockRoot(manager, document1, metric, degree, dataType);
            root.childRID = new ArrayList<>();
            root.childStartTime = new ArrayList<>();
            root.statistics = Statistics.newEmptyStats(dataType);
            return root;
        });

        // leaf node
        StatsBlockLeaf newLeaf = (StatsBlockLeaf) manager.newArcadeDocument(PREFIX_STATSBLOCK+metric, document2 ->{
            StatsBlockLeaf leaf = new StatsBlockLeaf(manager, document2, metric, degree, dataType, 0, true);
            leaf.dataList = new ArrayList<>();
            leaf.prevRID = manager.nullRID;
            leaf.succRID = manager.nullRID;
            return leaf;
        });

        newLeaf.save();

        // link leaf to root
        newTreeRoot.childRID.add(newLeaf.document.getIdentity());
        newTreeRoot.childStartTime.add(0L);

        newTreeRoot.save();

        return newTreeRoot;
    }

    public static StatsBlockRoot getStatsBlockRoot(ArcadeDocumentManager manager, RID rid, String metric) throws TimeseriesException {
        return (StatsBlockRoot) manager.getArcadeDocument(rid, document1 -> {
            Binary binary = new Binary(document1.getBinary("stat"));
            if (binary.getByte() == StatsBlockRoot.BLOCK_TYPE) {
                int degree = binary.getInt();
                DataType dataType = DataType.resolveFromBinary(binary);
                StatsBlockRoot root = new StatsBlockRoot(manager, document1, metric, degree, dataType);
                root.statistics = Statistics.getStatisticsFromBinary(dataType, binary);
                int childSize = binary.getInt();
                for (int i = 0; i < childSize; i++) {
                    root.childRID.add(manager.getRID(binary.getInt(), binary.getLong()));
                    root.childStartTime.add(binary.getLong());
                }
                return root;
            }else{
                throw new TimeseriesException("target StatsBlock is not root");
            }
        });
    }

    public static StatsBlock getStatsBlockNonRoot(ArcadeDocumentManager manager, RID rid, StatsBlock parent, String metric, int degree, DataType dataType, long startTime, boolean isLatest) throws TimeseriesException {
        StatsBlock statsBlock = (StatsBlock) manager.getArcadeDocument(rid, document1 -> {
            Binary binary = new Binary(document1.getBinary("stat"));
            switch (binary.getByte()){
                case StatsBlockInternal.BLOCK_TYPE -> {
                    StatsBlockInternal internal = new StatsBlockInternal(manager, document1, metric, degree, dataType, startTime, isLatest);
                    internal.statistics = Statistics.getStatisticsFromBinary(internal.dataType, binary);
                    int childSize = binary.getInt();
                    for (int i=0; i<childSize; i++){
                        internal.childRID.add(manager.getRID(binary.getInt(), binary.getLong()));
                        internal.childStartTime.add(binary.getLong());
                    }
                    return internal;
                }
                case StatsBlockLeaf.BLOCK_TYPE -> {
                    StatsBlockLeaf leaf = new StatsBlockLeaf(manager, document1, metric, degree, dataType, startTime, isLatest);
                    leaf.statistics = Statistics.getStatisticsFromBinary(leaf.dataType, binary);
                    leaf.prevRID = manager.getRID(binary.getInt(), binary.getLong());
                    leaf.succRID = manager.getRID(binary.getInt(), binary.getLong());
                    return leaf;
                }
                case StatsBlockRoot.BLOCK_TYPE -> {
                    throw new TimeseriesException("target StatsBlock is root");
                }
                default -> {
                    throw new TimeseriesException("target StatsBlock has invalid type");
                }
            }
        });
        // override cached object whose parent changed
        statsBlock.setParent(parent);
        statsBlock.startTime = startTime;
        statsBlock.isLatest = isLatest;
        return statsBlock;
    }

    public abstract void setParent(StatsBlock parent);

    /**
     * inesrt data point into statsBlock
     * @param data the data point to insert
     * @throws TimeseriesException
     */
    public abstract void insert(DataPoint data, boolean updateIfExist) throws TimeseriesException;

    // append statistics of out-of-order data point
    public abstract void appendStats(DataPoint data) throws TimeseriesException;

    // append statistics of latest child block
    public abstract void appendStats(Statistics stats) throws TimeseriesException;

    // update statistics of updated data point
    public abstract void updateStats(DataPoint oldDP, DataPoint newDP) throws TimeseriesException;

    public abstract void addChild(StatsBlock child) throws TimeseriesException;

    public abstract Statistics aggregativeQuery(long startTime, long endTime) throws TimeseriesException;

    public abstract DataPointSet periodQuery(long startTime, long endTime) throws TimeseriesException;
}
