package com.amazonaws.lambda.demo;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;

public class LambdaToDynDbHandler implements RequestHandler<SQSEvent, Void	> {

	@Override
	public Void handleRequest(SQSEvent event, Context context)
	{
		context.getLogger().log("===== Received Message count: " + event.getRecords().size());

		final AmazonDynamoDB dyndb = AmazonDynamoDBClientBuilder.defaultClient();
		
		HashMap<String,AttributeValue> item_values =
			    new HashMap<String,AttributeValue>();

        String tableName = "sqs_to_db_table";
		
		for(SQSMessage msg : event.getRecords()){
			String sqsMsg = msg.getBody() + "," + LocalDateTime.now();
			context.getLogger().log(sqsMsg);
			String[] Parsed = sqsMsg.split(",");
			String[] Fields = {"Counter", "Message", "SentTime", "LambdaTime"};
			//item_values.put("Counter", new AttributeValue( Parsed[0]) );
	        // ArrayList<String[]> extra_fields = new ArrayList<String[]>();
	        String[] fields = sqsMsg.split(",");
	        for (int c=0; c < fields.length; c++) {
                item_values.put(Fields[c], new AttributeValue(fields[c]));
                System.out.println(item_values);
            }
			try {
				dyndb.putItem(tableName, item_values);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}
}
