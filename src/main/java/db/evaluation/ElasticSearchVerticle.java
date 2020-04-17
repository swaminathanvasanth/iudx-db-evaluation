package db.evaluation;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

public class ElasticSearchVerticle extends AbstractVerticle {

  private static final Logger logger = LoggerFactory.getLogger(MongoDBVerticle.class);
  static long start = 0, end = 0;
  RestClient client;

  @Override
  public void start() {
    logger.info("ElasticSearch Verticle has started!");

    client = RestClient.builder(new HttpHost("localhost", 9200, "http")).build();

    vertx.eventBus().consumer("elastic", message -> search(message));
  }

  private void search(Message<Object> message) {

    try {
      Request request = new Request("GET", "/varanasi/_search");
      request.setJsonEntity(message.body().toString());

      long start = System.currentTimeMillis();
      Response response = client.performRequest(request);
      long end = System.currentTimeMillis();

      JsonObject responseJson = new JsonObject(EntityUtils.toString(response.getEntity()));

      logger.info("Measured time=" + (end - start));
      logger.info("Reported time=" + (responseJson.getInteger("took")));
      logger.info(
          "Hits="
              + (responseJson.getJsonObject("hits").getJsonObject("total").getInteger("value")));

      message.reply(responseJson);
    } catch (Exception e) {
      e.printStackTrace();
      logger.debug(e.getMessage());
      message.reply("failed");
    }
  }
}
