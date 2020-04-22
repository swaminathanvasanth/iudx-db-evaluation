package db.evaluation;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

public class APIServer extends AbstractVerticle {
  private static final Logger logger = Logger.getLogger(APIServer.class.getName());
  private final int port = 8443;
  private HttpServer server;
  private ClientAuth clientAuth;
  private String keystore, keystorePassword, truststore, truststorePassword;

  @Override
  public void start() throws Exception {

    Set<String> allowedHeaders = new HashSet<>();
    allowedHeaders.add("Accept");
    allowedHeaders.add("token");
    allowedHeaders.add("Content-Length");
    allowedHeaders.add("Content-Type");
    allowedHeaders.add("Host");
    allowedHeaders.add("Origin");
    allowedHeaders.add("Referer");
    allowedHeaders.add("Access-Control-Allow-Origin");

    Set<HttpMethod> allowedMethods = new HashSet<>();
    allowedMethods.add(HttpMethod.POST);

    Router router = Router.router(vertx);
    router
        .route()
        .handler(
            CorsHandler.create("*").allowedHeaders(allowedHeaders).allowedMethods(allowedMethods));
    router.route().handler(BodyHandler.create());

    router.post("/search/mongodb").handler(this::searchMongoDB);
    router.post("/search/elastic").handler(this::searchElastic);
    router.post("/search/timescale").handler(this::searchTimeScale);

    Properties prop = new Properties();
    InputStream input = null;

    try {

      input = new FileInputStream("config.properties");
      prop.load(input);

      keystore = prop.getProperty("keystore");
      keystorePassword = prop.getProperty("keystorePassword");

      truststore = prop.getProperty("truststore");
      truststorePassword = prop.getProperty("truststorePassword");

    } catch (IOException ex) {
      ex.printStackTrace();
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    clientAuth = ClientAuth.REQUEST;

    server =
        vertx.createHttpServer(
            new HttpServerOptions()
                .setSsl(true)
                .setClientAuth(clientAuth)
                .setTrustStoreOptions(
                    new JksOptions().setPath(truststore).setPassword(truststorePassword))
                .setKeyStoreOptions(
                    new JksOptions().setPath(keystore).setPassword(keystorePassword))
                .setCompressionSupported(true));

    server.requestHandler(router::accept).listen(port);

    logger.info("IUDX Connector started at Port : " + port + " !");
  }

  private void searchMongoDB(RoutingContext routingContext) {
    logger.info(" ---- API to MongoDB Client HIT ---- ");
    HttpServerResponse response = routingContext.response();
    JsonObject requested_data;
    DeliveryOptions options = new DeliveryOptions();
    requested_data = routingContext.getBodyAsJson();
    options.addHeader("options", requested_data.getString("options"));
    publishEvent("mongodb", requested_data, options, response);
  }

  private void searchElastic(RoutingContext routingContext) {
    //logger.info(" ---- API to Elastic Search Client HIT ---- ");
    HttpServerResponse response = routingContext.response();
    JsonObject requested_data;
    DeliveryOptions options = new DeliveryOptions();
    requested_data = routingContext.getBodyAsJson();
    options.addHeader("options", "options");
    publishEvent("elastic", requested_data, options, response);
  }

  private void searchTimeScale(RoutingContext routingContext) {
    logger.info(" ---- API to Time Scale Client HIT ---- ");
    HttpServerResponse response = routingContext.response();
    JsonObject requested_data;
    DeliveryOptions options = new DeliveryOptions();
    requested_data = routingContext.getBodyAsJson();
    options.addHeader("options", "options");
    publishEvent("timescale", requested_data, options, response);
  }

  private void publishEvent(
      String event,
      JsonObject requested_data,
      DeliveryOptions options,
      HttpServerResponse response) {

    vertx
        .eventBus()
        .request(
            event,
            requested_data,
            options,
            replyHandler -> {
              if (replyHandler.succeeded()) {
                String reply = replyHandler.result().body().toString();
                handle200(response, reply, requested_data);

              } else response.setStatusCode(400).end();
            });
  }

  private void handle200(HttpServerResponse response, String reply, JsonObject requested_data) {
    response
        .setStatusCode(200)
        .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
        .end(reply);
  }

  private void handle400(HttpServerResponse response) {
    response
        .setStatusCode(400)
        .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
        .end();
  }
}
