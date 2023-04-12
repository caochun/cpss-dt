package com.arcadedb.timeseries;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;

import java.util.LinkedHashMap;
import java.util.Map;

public class ArcadeVertexManager {
    // max size of cache
    public static final int MAX_CACHE_SIZE = 1024;
    // initial size of cache
    public static final int INIT_CACHE_SIZE = 64;

    // arcadeDB database
    public Database database;
    // null RID
    public RID nullRID;
    // LRU cache
    public LinkedHashMap<RID, ArcadeVertex> cache = new LinkedHashMap<>(INIT_CACHE_SIZE, 0.75f, true){
        @Override
        protected boolean removeEldestEntry(Map.Entry<RID, ArcadeVertex> eldest) {
            if (size() > MAX_CACHE_SIZE){
                ArcadeVertex eldestVertex = eldest.getValue();
                eldestVertex.cahced = false;
                eldestVertex.save();
                return true;
            }
            return false;
        }
    };

    public ArcadeVertexManager(Database database){
        this.database = database;
        nullRID = new RID(database, -1, -1);
    }

    public RID getRID(int bucketId, long offset){
        return new RID(database, bucketId, offset);
    }

    // create new ArcadeVertex
    public ArcadeVertex newArcadeVertex(String vertexType, ArcadeVertexBuilder builder) throws TimeseriesException {
        ArcadeVertex newVertex = builder.build(database.newVertex(vertexType));
        newVertex.setAsDirty();

        cache.put(newVertex.vertex.getIdentity(), newVertex);
        newVertex.cahced = true;

        return newVertex;
    }

    // get existing vertex from arcadeDB
    public ArcadeVertex getArcadeVertex(RID rid, ArcadeVertexBuilder builder) throws TimeseriesException {
        ArcadeVertex result;
        if ((result = cache.get(rid)) != null){
            return result;
        }
        result = builder.build(database.lookupByRID(rid, true).asVertex());

        cache.put(rid, result);
        result.cahced = true;

        return result;
    }

    // save all dirty vertexes
    public void saveAll(){
        for (ArcadeVertex vertex : cache.values()){
            vertex.save();
        }
    }

    public void clearCache(){
        cache.clear();
    }
}
