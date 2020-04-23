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
		
		JsonObject config=new JsonObject().put("username","user").put("password","12345678")
					.put("authSource","as").put("host","localhost")
					.put("port",54321).put("db_name","db");
		client = MongoClient.createShared(vertx, config);

		vertx.eventBus().consumer("mongodb", message -> {
			String options = message.headers().get("options");
			if (options.equalsIgnoreCase("find")) {
				searchFind(message);
			}else if(options.equalsIgnoreCase("aggregate")){
				searchAggregate(message);
			} 
			else
				message.reply("Invalid Command");
		});
	}

	private void searchFind(Message<Object> message){
		//MongoFind Query
		
		JsonObject query;
		int limit=10;
		FindOptions findOptions=new FindOptions();
		//sorted response; 
		findOptions.setSort(new JsonObject().put("__time",-1));
		findOptions.setFields(new JsonObject().put("_id",0));
		try{
			JsonObject request=(JsonObject)message.body();
			limit=request.getInteger("limit");
			findOptions.setLimit(limit);
			String queryString=request.getString("query");
			query=new JsonObject(queryString);
			logger.info("QUERY: "+query.toString());
			start=System.currentTimeMillis();
			client.findWithOptions(COLLECTION,query,findOptions,response->{
			if(response.succeeded()){
				end=System.currentTimeMillis();
				//JsonArray res=new JsonArray();
				//for(JsonObject doc: response.result())
				//	res.add(doc);
				logger.info("Query succeeded with "+response.result().size()+" in "+(end-start)+" mills.");
				message.reply(response.result().toString());
			} else{
				response.cause().printStackTrace();
				message.fail(0,"failed");
			}
			});
		}catch (Exception e){
			e.printStackTrace();
			message.fail(0,"failed");
		}
		
	}

	private void searchAggregate(Message<Object> message){
	
		//MongoAggregation Query
		
		String pipelineString;
		try{
			JsonObject request=(JsonObject)message.body();
			pipelineString=request.getString("query");
			JsonArray pipeline=new JsonArray(pipelineString);
			JsonObject command=new JsonObject().put("aggregate","archive").put("pipeline",pipeline)
						.put("cursor",new JsonObject().put("batchSize",10000));
			start=System.currentTimeMillis();
			client.runCommand("aggregate",command, res->{
				if(res.succeeded()){
					end=System.currentTimeMillis();
					logger.info("Query succeeded with "+res.result().getJsonObject("cursor").getJsonArray("firstBatch").size()+ " returned documents in "+(end-start)+" mills.");
					message.reply(res.result().getJsonObject("cursor").getJsonArray("firstBatch").toString());
				} else{
					res.cause().printStackTrace();
					message.fail(0,"failed");
				}
			});
		} catch(Exception e){
			e.printStackTrace();
			message.fail(0,"failed");
		}
	}
}
