package cloud_sacc.resource;

import java.util.ArrayList;
import java.util.Iterator;

import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.apache.commons.lang.reflect.ConstructorUtils;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Injector;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * Base Class of All JAX-RS Resources
 * 
 * Use this as a suitable Extension Point :)
 */
public class BaseResource {
	public static final String ID_MASK = "{ id: [^/]+ }";
	
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Inject
	protected ObjectMapper objectMapper;

	@Inject
	protected Injector injector;
	
	protected <K extends BaseResource> K createResource(Class<K> clazz, Object... args) throws Exception {
		if (null != injector.getBinding(clazz))
			return injector.getInstance(clazz);
		
		@SuppressWarnings("unchecked")
		K result = (K) ConstructorUtils.invokeConstructor(clazz, args);
		
		injector.injectMembers(result);
		
		return result;
	}
	
	@GET
	@Produces("text/plain")
	@Path("/info")
	public String getResourceClass() {
		BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAI2NJOYGLLNEPDQ7Q", "939d+7LXLR+537gT1EkY8AiRyBjwuPZdq61VRLZL");
		String myQueueUrl = "https://sqs.us-east-1.amazonaws.com/537806310701/awseb-e-p2fuk9kuqp-stack-AWSEBWorkerQueue-1943JQQ2ET7VB";
		AmazonSQS sqs = new AmazonSQSClient(awsCreds);
		
		//workerBD queue
		sqs.sendMessage(new SendMessageRequest()
				.withQueueUrl(myQueueUrl)
				.withMessageBody("This is my message text."));
		sqs.sendMessage(new SendMessageRequest()
		.withQueueUrl("https://sqs.us-east-1.amazonaws.com/537806310701/awseb-e-jfusv3qg9v-stack-AWSEBWorkerQueue-1QHVKJWLZIK5A")
		.withMessageBody("This is my message text."));

		String t ="-\n";
		int steps=0;
		try{
		//
		DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(awsCreds));
			
		//creation
	/*try{ArrayList<AttributeDefinition> attributeDefinitions= new ArrayList<AttributeDefinition>();
		attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType("N"));
		ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
		keySchema.add(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH));
		CreateTableRequest request = new CreateTableRequest()
				.withTableName("ProductCatalog")
				.withKeySchema(keySchema)
				.withAttributeDefinitions(attributeDefinitions)
				.withProvisionedThroughput(new ProvisionedThroughput()
				    .withReadCapacityUnits(5L)
					.withWriteCapacityUnits(6L));
		Table tabless = dynamoDB.createTable(request);
		tabless.waitForActive();
		steps++;
	}catch(Exception e){t+="debutcreat\n"+e+"\n";}
*/
		//retrieve
		steps=1;

		
			//listing
			TableCollection<ListTablesResult> tables = dynamoDB.listTables();
			Iterator<Table> iterator = tables.iterator();
			steps=2;
			Table table=null;
			while (iterator.hasNext()) {
				Table tabl = iterator.next();
				table=tabl;
				t+=(tabl.getTableName())+"\n";
			}
			steps=4;
			
			Item item = new Item()
		    .withPrimaryKey("Id", 2052)
		    .withString("Title", "20-Bicycle 205")
		    .withString("Description", "205 description")
		    .withString("BicycleType", "Hybrid")
		    .withString("BicycleTypeqsdsq", "Hybrid")
		    .withString("Brands", "Brand-Company C");
		    
			// Write the item to the table 
			PutItemOutcome outcome = table.putItem(item);
			
		}catch(Exception e){t+="failed creating tables\n"+e+"\n";}	
		//return getClass().getName()+"\nAllez verifier la queue_\n"+t+"\n"+steps;
		return getClass().getName()+"_mock1\n";
	}
	
	@GET
	@Produces("text/plain")
	@Path("/request")
	public String handleRequest(@DefaultValue("") @QueryParam("cmd") String cmd, 
								@DefaultValue("") @QueryParam("arg") String arg){
		BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAI2NJOYGLLNEPDQ7Q", "939d+7LXLR+537gT1EkY8AiRyBjwuPZdq61VRLZL");
		String workerBDQueueUrl = "https://sqs.us-east-1.amazonaws.com/537806310701/awseb-e-jfusv3qg9v-stack-AWSEBWorkerQueue-1QHVKJWLZIK5A";
		AmazonSQS sqs = new AmazonSQSClient(awsCreds);
		
		String feedback="initial";
		try{
			//workerBD queue, une suele instance donc meme si un replicat en envoei un autre pas grave, juste penser à les vider
			sqs.sendMessage(new SendMessageRequest()
			.withQueueUrl(workerBDQueueUrl)
			.withMessageBody("Just to start and persist public tweet stream in dynamoDB"));
			feedback="workerBD started";
			//gerer le retour/notification par mail? Donc recuperer adresse
			//envoie la requete au worker qui va executer la commande
			sqs.sendMessage(new SendMessageRequest()
			.withQueueUrl("https://sqs.us-east-1.amazonaws.com/537806310701/awseb-e-p2fuk9kuqp-stack-AWSEBWorkerQueue-1943JQQ2ET7VB")
			.withMessageBody(cmd+"-"+arg));
			feedback="request processing started";
		}catch(Exception e){feedback+="\nprobleme"+e+"\n";}	
		//return getClass().getName()+"\nAllez verifier la queue_\n"+t+"\n"+steps;
		return getClass().getName()+"\n"+feedback+"ok";
	}
}
