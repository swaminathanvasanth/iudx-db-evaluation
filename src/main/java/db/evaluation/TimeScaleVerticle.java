package db.evaluation;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

import java.util.List;


public class TimeScaleVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(MongoDBVerticle.class);
    static long start = 0, end = 0;
    PgPool client;

    @Override
    public void start() {
        PgConnectOptions connectOptions = new PgConnectOptions()
            .setPort(5432)
            .setHost("localhost")
            .setDatabase("public")
            .setUser("postgres")
            .setPassword("12345678");

        // Pool options
        PoolOptions poolOptions = new PoolOptions()
            .setMaxSize(5);

        // Create the client pool
        client = PgPool.pool(vertx, connectOptions, poolOptions);
        logger.info("Timescale Verticle has started!");
        vertx.eventBus().consumer("timescale", message -> search(message));
    }

    private void search(Message<Object> message) {

        try {
            JsonObject jsonq = new JsonObject(message.body().toString());
            String query     = jsonq.getString("query");
            start            = System.currentTimeMillis();

            client
                .query(query)
                .execute(ar -> {
                    if (ar.succeeded()) {

                        end                 = System.currentTimeMillis();
                        JsonArray res       = new JsonArray();
                        RowSet<Row> rows    = ar.result();

                        logger.info("Measured time = " + (end - start));
                        logger.info("Query succeeded with " + rows.size() + " returned documents in "+ (end-start) +" mills.");

                        for (Row row : rows) {
                            res.add(row.getValue(0));
                        }

                        message.reply(res);

                    } else {
                        System.out.println("Failure: " + ar.cause().getMessage());
                    }
                });
        } catch (Exception e) {
            e.printStackTrace();
            logger.debug(e.getMessage());
            message.reply("failed");
        }
    }
}
