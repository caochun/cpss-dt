package com.arcadedb.test;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.timeseries.*;
import indi.hjhk.log.Logger;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

public class TimeseriesOODTest {
    public static void main(String[] args) {
        DatabaseFactory dbf = new DatabaseFactory("./databases/tsTest");

        Database database;
        if (dbf.exists()){
            database = dbf.open();
        }else{
            database = dbf.create();
        }

        database.begin();
        if (!database.getSchema().existsType("test")){
            database.getSchema().createVertexType("test");
        }
        Vertex testVertex = database.newVertex("test").save();
        database.commit();

        Logger.logOnStdout("created vertex rid is "+testVertex.getIdentity());
        TimeseriesEngine tsEngine = new TimeseriesEngine(database);

        tsEngine.begin();
        try {
            long startTime = System.currentTimeMillis();

            final int testSize = 123456789;
            final int commitSize = 1000000;
            final int oodProb = 10;

            Random ran = new Random();

            Queue<Integer> oodData = new LinkedList<>();
            long count = 0;
            long periodStartTime = System.currentTimeMillis();

            for (int i=0; i<testSize; i++){
                if (ran.nextInt(100) < oodProb){
                    oodData.offer(i);
                }else {
                    tsEngine.insertDataPoint(testVertex, "status", new DataType(DataType.BaseType.LONG, 0), new LongDataPoint(i, i));
                    count++;
                }
                if (count == commitSize){
                    tsEngine.commit();

                    long periodElapsed = System.currentTimeMillis() - periodStartTime;
                    periodStartTime = System.currentTimeMillis();
                    Logger.logOnStdout("inserted %d datapoints using %d ms", count, periodElapsed);

                    count = 0;
                    tsEngine.begin();
                }
            }

            Logger.logOnStdout("start to insert out-of-order datapoint");

            long oodSize = oodData.size();
            while (!oodData.isEmpty()){
                int ood = oodData.poll();
                tsEngine.insertDataPoint(testVertex, "status", new DataType(DataType.BaseType.LONG, 0), new LongDataPoint(ood, ood));
                count++;

                if (count == commitSize){
                    tsEngine.commit();

                    long periodElapsed = System.currentTimeMillis() - periodStartTime;
                    periodStartTime = System.currentTimeMillis();
                    Logger.logOnStdout("inserted %d datapoints using %d ms", count, periodElapsed);

                    count = 0;
                    tsEngine.begin();
                }
            }

            tsEngine.commit();

            long elapsed = System.currentTimeMillis() - startTime;
            Logger.logOnStdout("inserted %d datapoints including %d out-of-order ones using %d ms", testSize, oodSize, elapsed);

            tsEngine.begin();
            for (int i=0; i<20; i++){
                int queryStart = ran.nextInt(testSize);
                int queryEnd = ran.nextInt(queryStart, testSize);
                long ans = (long) (queryEnd + queryStart) * (queryEnd - queryStart + 1) / 2;

                startTime = System.currentTimeMillis();

                LongStatistics statistics = (LongStatistics) tsEngine.aggregativeQuery(testVertex, "status", queryStart, queryEnd);
                long sum = statistics.sum;
                elapsed = System.currentTimeMillis() - startTime;
                Logger.logOnStdout("query [%d, %d] get %s in %d ms with correctSum=%d, correct=%s", queryStart, queryEnd, statistics.toPrettyPrintString(), elapsed, ans, sum == ans);
            }
        } catch (TimeseriesException e) {
            e.printStackTrace();
            tsEngine.rollback();
            database.close();
            return;
        }
        tsEngine.commit();

        database.close();
    }
}
