
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.regions.Regions;



public class DynamoTest{
    
    public static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
	public static DynamoDB dynamodb = new DynamoDB(client);

	public static void main(String[] args) throws Exception {

		String tableName = "Java_created_table";

		Table workingTable = createSomeTable(tableName);
		Scanner scan = new Scanner(System.in);
		while (true){
			System.out.println("Choose the command to perform");
			String command = scan.nextLine();
			if(command.equals("put")){
				System.out.println("Enter Id and Name for the item(new line for each value): ");
				int id = scan.nextInt();
				String name = scan.next();
				putItem(id, name, workingTable);

			} else if(command.equals("read")){
				System.out.println("To read item from table, enter the id:");
				int id = scan.nextInt();
				Item item = readItem(id, workingTable);
				System.out.println(item.toJSONPretty());


			} else if(command.equals("exit")){
				break;
			} else {
				System.out.println("Looks like you entered wrong command. Three commands are performed: put, read, exit");
			}
        
		}
		scan.close();
		System.exit(0);			
	}

	public static Table createSomeTable(String tname){


			System.out.println("Starting dynamoDB table creation...");

			List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
			attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType("N"));
			
			List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
			keySchema.add(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH));

			try {

			CreateTableRequest request = new CreateTableRequest()
					.withTableName(tname)
					.withKeySchema(keySchema)
					.withAttributeDefinitions(attributeDefinitions)
					.withProvisionedThroughput(new ProvisionedThroughput()
						.withReadCapacityUnits(10L)
						.withWriteCapacityUnits(10L));

			Table table = dynamodb.createTable(request);

			table.waitForActive();
		    }
		    catch (ResourceInUseException invokex){
		    	System.out.println("-------------------------------------------------");
		    	System.out.println("DynamoDB table you want to create already exists.");
		    	System.out.println("-------------------------------------------------");
		    }
		    finally{
		    	Table workingTable = dynamodb.getTable(tname);
		    	System.out.println("Yours dynamoDB table is ready for work.");
		    	return workingTable;
		    }

	}

	public static void putItem(int id, String name, Table table){

		Item item = new Item()
			.withPrimaryKey("Id", id)
			.withString("Name", name);

        PutItemOutcome put = table.putItem(item);
        System.out.println("Item was written to the table.");
	}

	public static Item readItem(int id, Table table){

		Item item = table.getItem("Id", id);
		return item;
	}

}