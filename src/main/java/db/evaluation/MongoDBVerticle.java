package db.evaluation;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class MongoDBVerticle extends AbstractVerticle {

	private static final Logger logger = LoggerFactory.getLogger(MongoDBVerticle.class);

	@Override
	public void start() throws Exception {
		logger.info("MongoDB Search Verticle started!");

		vertx.eventBus().consumer("mongodb", message -> {
			String options = message.headers().get("options");
			if (options.equalsIgnoreCase("PING")) {
				message.reply("PONG");
			} else
				message.reply("Invalid Command");

		});
	}
}
