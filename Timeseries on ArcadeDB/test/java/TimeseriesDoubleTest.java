import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Vertex;
import indi.hjhk.exception.ExceptionSerializer;
import indi.hjhk.log.Logger;
import nju.hjh.arcadedb.timeseries.DataType;
import nju.hjh.arcadedb.timeseries.TimeseriesEngine;
import nju.hjh.arcadedb.timeseries.datapoint.DoubleDataPoint;
import nju.hjh.arcadedb.timeseries.datapoint.LongDataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.DoubleStatistics;
import nju.hjh.arcadedb.timeseries.statistics.LongStatistics;

import java.util.Random;

public class TimeseriesDoubleTest {
    public static void main(String[] args) {
        Logger logger = Logger.getPureLogger();
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

        logger.logOnStdout("created vertex rid is "+testVertex.getIdentity());
        TimeseriesEngine tsEngine = new TimeseriesEngine(database);

        tsEngine.begin();
        try {
            long startTime = System.currentTimeMillis();

            final int testSize = 10000000;
            final int commitSize = 1000000;

            Random ran = new Random();

            long periodStartTime = System.currentTimeMillis();

            for (int i=0; i<testSize; i++){
                if (i > 0 && i % commitSize == 0) {
                    tsEngine.commit();

                    long periodElapsed = System.currentTimeMillis() - periodStartTime;
                    periodStartTime = System.currentTimeMillis();
                    logger.logOnStdout("inserted datapoints range=[%d, %d) using %d ms", i-commitSize , i, periodElapsed);

                    tsEngine.begin();
                }
                tsEngine.insertDataPoint(testVertex, "status", DataType.DOUBLE, new DoubleDataPoint(i, i/16.0), false);
            }

            tsEngine.commit();

            long elapsed = System.currentTimeMillis() - startTime;
            logger.logOnStdout("insert "+testSize+" datapoints into status of testVertex using "+elapsed+" ms");

            tsEngine.begin();

            for (int i=0; i<20; i++){
                int queryStart = ran.nextInt(testSize);
                int queryEnd = ran.nextInt(queryStart, testSize);
                double ans = (long) (queryEnd + queryStart) * (queryEnd - queryStart + 1) / 32.0;

                startTime = System.currentTimeMillis();

                DoubleStatistics statistics = (DoubleStatistics) tsEngine.aggregativeQuery(testVertex, "status", queryStart, queryEnd);
                double sum = statistics.sum;
                elapsed = System.currentTimeMillis() - startTime;
                logger.logOnStdout("query [%d, %d] get %s in %d ms with correctSum=%f, correct=%s", queryStart, queryEnd, statistics.toPrettyPrintString(), elapsed, ans, sum == ans);
            }

        } catch (TimeseriesException e) {
            logger.logOnStderr(ExceptionSerializer.serializeAll(e));
            tsEngine.rollback();
            database.close();
            return;
        }
        tsEngine.commit();

        database.close();
    }
}
