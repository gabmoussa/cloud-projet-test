package cloud_sacc.resource;

import java.util.Random;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import twitter4j.StallWarning;
import twitter4j.StatusListener;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;

import javax.ws.rs.Path;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import twitter4j.Status;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

@Path("/sqsWorkerBD")
public class WorkerPeuplerBD extends AbstractSQSDResource {
    private final Logger logger = LoggerFactory.getLogger(WorkerPeuplerBD.class);
    Random random = new Random(23);
    @Override
    protected Response executeInternal(HttpHeaders headers, String bodyNode) {
        logger.info("headers: {}, body {}", headers, bodyNode);
        logger.debug("***********nopenope*********\n");
        
        BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAI2NJOYGLLNEPDQ7Q", "939d+7LXLR+537gT1EkY8AiRyBjwuPZdq61VRLZL");
        DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(awsCreds));
    	Table tweet = dynamoDB.getTable("Tweet");
    	Item item = new Item()
	    .withPrimaryKey("id",random.nextInt(Integer.MAX_VALUE))
	    .withString("hashtag", "hashtagtest")
	    .withString("region", "JAP");
	    // Write the item to the table 
		PutItemOutcome outcome = tweet.putItem(item);
    	
        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		StatusListener listener = new StatusListener(){
	        BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAI2NJOYGLLNEPDQ7Q", "939d+7LXLR+537gT1EkY8AiRyBjwuPZdq61VRLZL");
        	DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(awsCreds));
        	Table tweet = dynamoDB.getTable("Tweet");
        	public void onStatus(twitter4j.Status status) {
	        	//System.out.println(status.getUser().getName() + " : " + status.getText());
	            Item item = new Item()
	            .withPrimaryKey("id",random.nextInt(Integer.MAX_VALUE))
	    	    .withString("hashtag", status.getText())
	    	    .withString("region", status.getGeoLocation()!=null ? status.getGeoLocation().toString() : "unknow");
	    	    // Write the item to the table 
	    		PutItemOutcome outcome = tweet.putItem(item);
	        }
	        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
	        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
	        public void onException(Exception ex) {
	            ex.printStackTrace();
	        }
			@Override
			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub
				
			}
			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
				
			}
	    };
	    String token ="3037570211-fZBzmQpR3uDPMdKZl6qWNUHsJ9QAtfW9T8BdXpN", secretToken="NGNwIHAKnHzI1pkDvHpjoe4WbGA7Yb7gnXESr410156kz", consumerKey="m5fPsGrHQnr9Bc6mkIRX4ooCm", secretConsumerKey="vPoRllZMDpY0OEr3DtqgMUfODRZOgID4D1sGrxI3JBH9shaISB";
	    AccessToken at = new AccessToken(token, secretToken);
	    twitterStream.setOAuthConsumer(consumerKey,secretConsumerKey);
	    twitterStream.setOAuthAccessToken(at);
		twitterStream.addListener(listener);
		twitterStream.sample();//commence à écouter extrait flux public
        
		return Response.ok().build();
    }
}