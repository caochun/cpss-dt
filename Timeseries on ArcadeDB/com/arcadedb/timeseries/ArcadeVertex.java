package com.arcadedb.timeseries;

import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;

public abstract class ArcadeVertex {
    // manager of this vertex
    public final ArcadeVertexManager manager;
    // arcadeDB vertex
    public final Vertex vertex;
    // if this vertex is dirty
    public boolean dirty = false;
    public boolean cahced = false;

    public ArcadeVertex(ArcadeVertexManager manager, Vertex vertex){
        this.manager = manager;
        this.vertex = vertex;
    }

    // save ArcadeDB vertex
    public void save(){
        if (dirty){
            try {
                serializeVertex().save();
            } catch (TimeseriesException e) {
                throw new RuntimeException(e);
            }
            dirty = false;
            // put new vertex into cache
            if (!cahced)
                manager.putCache(this);
        }
    }

    // put data into ArcadeDB vertex
    public abstract MutableVertex serializeVertex() throws TimeseriesException;

    // set this vertex to dirty
    public void setAsDirty(){
        // ensure vertex is in cache
        if (!cahced)
            manager.putCache(this);
        dirty = true;
    }
}
