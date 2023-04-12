package com.arcadedb.timeseries;

import com.arcadedb.database.Binary;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;

import java.util.ArrayList;

public abstract class StatsBlock extends ArcadeVertex{
    public static final int DEFAULT_TREE_DEGREE = 10;
    public static final int MAX_DATA_BLOCK_SIZE = 4096;
    public static final String PREFIX_STATSBLOCK = "_stat_";

    /**
     * size of one child block
     * childRID(12B) + childStartTime(8B)
     */
    public static final int CHILD_SIZE = 20;

    public final String measurement;
    public final int degree;
    public final DataType dataType;
    public long startTime;
    public Statistics statistics;
    public boolean isLatest = false;

    public StatsBlock(ArcadeVertexManager manager, Vertex vertex, String measurement, int degree, DataType dataType, long startTime, boolean isLatest){
        super(manager, vertex);
        this.measurement = measurement;
        this.degree = degree;
        this.dataType = dataType;
        this.startTime = startTime;
        this.isLatest = isLatest;
    }

    public static StatsBlockRoot newStatsTree(ArcadeVertexManager manager, String measurement, DataType dataType) throws TimeseriesException {
        return newStatsTree(manager, measurement, dataType, DEFAULT_TREE_DEGREE);
    }

    public static StatsBlockRoot newStatsTree(ArcadeVertexManager manager, String measurement, DataType dataType, int degree) throws TimeseriesException {
        // root node
        StatsBlockRoot newTreeRoot = (StatsBlockRoot) manager.newArcadeVertex(PREFIX_STATSBLOCK+measurement, vertex1 -> {
            StatsBlockRoot root = new StatsBlockRoot(manager, vertex1, measurement, degree, dataType);
            root.childRID = new ArrayList<>();
            root.childStartTime = new ArrayList<>();
            root.statistics = Statistics.newEmptyStats(dataType);
            return root;
        });

        // leaf node
        StatsBlockLeaf newLeaf = (StatsBlockLeaf) manager.newArcadeVertex(PREFIX_STATSBLOCK+measurement, vertex2 ->{
            StatsBlockLeaf leaf = new StatsBlockLeaf(manager, vertex2, measurement, degree, dataType, 0, true);
            leaf.dataList = new ArrayList<>();
            leaf.prevRID = manager.nullRID;
            leaf.succRID = manager.nullRID;
            return leaf;
        });

        newLeaf.save();

        // link leaf to root
        newTreeRoot.childRID.add(newLeaf.vertex.getIdentity());
        newTreeRoot.childStartTime.add(0L);

        newTreeRoot.save();

        return newTreeRoot;
    }

    public static StatsBlockRoot getStatsBlockRoot(ArcadeVertexManager manager, RID rid, String measurement) throws TimeseriesException {
        return (StatsBlockRoot) manager.getArcadeVertex(rid, vertex1 -> {
            Binary binary = new Binary(vertex1.getBinary("stat"));
            if (binary.getByte() == StatsBlockRoot.BLOCK_TYPE) {
                int degree = binary.getInt();
                DataType dataType = DataType.resolveFromBinary(binary);
                StatsBlockRoot root = new StatsBlockRoot(manager, vertex1, measurement, degree, dataType);
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

    public static StatsBlock getStatsBlockNonRoot(ArcadeVertexManager manager, RID rid, StatsBlock parent, long startTime, boolean isLatest) throws TimeseriesException {
        StatsBlock statsBlock = (StatsBlock) manager.getArcadeVertex(rid, vertex1 -> {
            Binary binary = new Binary(vertex1.getBinary("stat"));
            switch (binary.getByte()){
                case StatsBlockInternal.BLOCK_TYPE -> {
                    StatsBlockInternal internal = new StatsBlockInternal(manager, vertex1, parent.measurement, parent.degree, parent.dataType, startTime, isLatest);
                    internal.statistics = Statistics.getStatisticsFromBinary(internal.dataType, binary);
                    int childSize = binary.getInt();
                    for (int i=0; i<childSize; i++){
                        internal.childRID.add(manager.getRID(binary.getInt(), binary.getLong()));
                        internal.childStartTime.add(binary.getLong());
                    }
                    return internal;
                }
                case StatsBlockLeaf.BLOCK_TYPE -> {
                    StatsBlockLeaf leaf = new StatsBlockLeaf(manager, vertex1, parent.measurement, parent.degree, parent.dataType, startTime, isLatest);
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
        // override cached object whose parent was unknown
        statsBlock.setParent(parent);
        statsBlock.startTime = startTime;
        statsBlock.isLatest = isLatest;
        return statsBlock;
    }

    public abstract void setParent(StatsBlock parent);

    public abstract void insert(DataPoint data) throws TimeseriesException;

    public abstract void appendStats(Statistics stats) throws TimeseriesException;

    public abstract void addChild(StatsBlock child) throws TimeseriesException;

    public abstract Statistics aggregativeQuery(long startTime, long endTime) throws TimeseriesException;

    public abstract DataPointSet nonAggregativeQuery(long startTime, long endTime);
}
