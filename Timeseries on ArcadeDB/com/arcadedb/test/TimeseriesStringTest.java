package com.arcadedb.test;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.timeseries.*;
import indi.hjhk.log.Logger;

import java.util.ArrayList;
import java.util.Random;

public class TimeseriesStringTest {

    /**
     * from stack overflow
     * <a href=https://stackoverflow.com/questions/2863852/how-to-generate-a-random-string-in-java>How to generate a random String in Java</a>
     */
    public static String generateString(Random rng, String characters, int length)
    {
        char[] text = new char[length];
        for (int i = 0; i < length; i++)
        {
            text[i] = characters.charAt(rng.nextInt(characters.length()));
        }
        return new String(text);
    }

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

            final int testSize = 10000000;
            final int commitSize = 1000000;
            final String charUsed = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            final int strLen = 10;

            ArrayList<String> strList = new ArrayList<>();

            Random ran = new Random();

            for (int i=0; i<testSize; i++){
                strList.add(generateString(ran, charUsed, strLen));
            }

            long periodStartTime = System.currentTimeMillis();

            for (int i=0; i<testSize; i++){
                if (i > 0 && i % commitSize == 0) {
                    tsEngine.commit();

                    long periodElapsed = System.currentTimeMillis() - periodStartTime;
                    periodStartTime = System.currentTimeMillis();
                    Logger.logOnStdout("inserted datapoints range=[%d, %d) using %d ms", i-commitSize , i, periodElapsed);

                    tsEngine.begin();
                }
                tsEngine.insertDataPoint(testVertex, "status", new DataType(DataType.BaseType.STRING, 40), new StringDataPoint(i, strList.get(i)));
            }

            tsEngine.commit();

            long elapsed = System.currentTimeMillis() - startTime;
            Logger.logOnStdout("insert "+testSize+" datapoints into status of testVertex using "+elapsed+" ms");

            tsEngine.begin();

            for (int i=0; i<20; i++){
                int queryStart = ran.nextInt(testSize);
                int queryEnd = ran.nextInt(queryStart, testSize);

                startTime = System.currentTimeMillis();
                StringStatistics statistics = (StringStatistics) tsEngine.aggregativeQuery(testVertex, "status", queryStart, queryEnd);
                elapsed = System.currentTimeMillis() - startTime;
                Logger.logOnStdout("query [%d, %d] get statistics{\n\tcount=%d\n\tfirstTime=%d\n\tfirstValue=%s\n\tlastTime=%d\n\tlastValue=%s\n} in %d ms with correct=%s",
                        queryStart, queryEnd, statistics.count, statistics.firstTime, statistics.firstValue, statistics.lastTime, statistics.lastValue, elapsed,
                        (strList.get((int) statistics.firstTime).equals(statistics.firstValue) && strList.get((int) statistics.lastTime).equals(statistics.lastValue)));
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
