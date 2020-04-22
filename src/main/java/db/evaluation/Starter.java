package db.evaluation;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Launcher;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class Starter extends AbstractVerticle {

	String name;
	public final static Logger logger = LoggerFactory.getLogger(Starter.class);

	public static void main(String[] args) {
		Launcher.executeCommand("run", Starter.class.getName());
	}

	@Override
	public void start(Future<Void> startFuture) throws Exception {
		final Future<Void> future = Future.future();
		int procs = Runtime.getRuntime().availableProcessors();

		vertx.deployVerticle(APIServer.class.getName(), new DeploymentOptions().setWorker(true).setInstances(procs * 2),
				res -> {
					if (res.succeeded()) {
						logger.info("Deployed APIServer Verticle");
						vertx.deployVerticle(TimeScaleVerticle.class.getName(), dbresponse -> {
							if (dbresponse.succeeded()) {
								logger.info("Deployed Timescale Verticle");
								future.complete();
							} else {
								logger.fatal("Failed to deploy verticle " + dbresponse.cause());
								future.fail(dbresponse.cause());
							}
						});
					} else {
						logger.fatal("Failed to deploy verticle " + res.cause());
						future.fail(res.cause());
					}
				});
	}
}
