package db.evaluation;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.eventbus.Message;
import io.vertx.ext.mongo.FindOptions;

public class MongoDBVerticle extends AbstractVerticle {

	private static final Logger logger = LoggerFactory.getLogger(MongoDBVerticle.class);
	private static final String COLLECTION="archive";
	MongoClient client;
	static long start=0,end=0;
	@Override
	public void start() throws Exception {
		logger.info("MongoDB Search Verticle started!");
		
		JsonObject config=new JsonObject();
		client = MongoClient.createShared(vertx, config);

		vertx.eventBus().consumer("mongodb", message -> {
			String options = message.headers().get("options");
			if (options.equalsIgnoreCase("mongo")) {
				search(message);
			} else
				message.reply("Invalid Command");

		});
	}

	private void search(Message<Object> message){
		//MongoFind Query
		
		JsonObject query;
		FindOptions findOptions=new FindOptions();
		//sorted response; 
		//limit ranges in order of [10, 100, 1000, 10000]
		findOptions.setSort(new JsonObject().put("__time",-1));
		findOptions.setLimit(1000);
		findOptions.setFields(new JsonObject().put("_id",0));
		//paste the queries from the Google docs
		//Make sure the quotes in the json file are properly formatted using backslashes
		String queryString="{}";
		try{
			query=new JsonObject(queryString);
			logger.info("QUERY: "+query.toString());
			start=System.currentTimeMillis();
			client.findWithOptions(COLLECTION,query,findOptions,response->{
			if(response.succeeded()){
				end=System.currentTimeMillis();
				JsonArray res=new JsonArray();
				for(JsonObject doc: response.result())
					res.add(doc);
				logger.info("Query succeeded with "+ res.size()+ " returned documents in "+(end-start)+" mills.");
				message.reply(res);
			} else{
				response.cause().printStackTrace();
				message.fail(0,"failed");
			}
			});
		}catch (Exception e){
			e.printStackTrace();
			message.fail(0,"failed");
		}
		
		//MongoAggregation Query
		
		//String pipelineString="[]";
		//try{
		//	JsonArray pipeline=new JsonArray(pipelineString);
		//	JsonObject command=new JsonObject().put("aggregate","archive").put("pipeline",pipeline)
		//				.put("cursor",new JsonObject().put("batchSize",100));
		//	start=System.currentTimeMillis();
		//	client.runCommand("aggregate",command, res->{
		//		if(res.succeeded()){
		//			end=System.currentTimeMillis();
		//			JsonArray result = res.result().getJsonObject("cursor").getJsonArray("firstBatch");
		//			JsonArray response=new JsonArray();
		//			for (Object o : result) {
		//					JsonObject j = (JsonObject) o;
		//					response.add(j);
		//			}
		//			logger.info("Query succeeded with "+ response.size()+ " returned documents in "+(end-start)+" mills.");
		//			message.reply(response);
		//		} else{
		//			res.cause().printStackTrace();
		//			message.fail(0,"failed");
		//		}
		//	});
		//} catch(Exception e){
		//	e.printStackTrace();
		//	message.fail(0,"failed");
		//}
	}
}
