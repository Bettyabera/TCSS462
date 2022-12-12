package lambda;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import saaf.Inspector;
import saaf.Response;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.Properties;


import java.util.HashMap;
import java.util.Scanner;


public class Load implements RequestHandler<Request, HashMap<String, Object>>
{
    
    String bucketname;
    String filename;
    String schemaname = "term_project";
    String tablename = "SalesDB";


    // Lambda Function Handler
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        LambdaLogger logger = context.getLogger();
        Inspector inspector = new Inspector();

        try
        {
            Properties properties = new Properties();
            properties.load(new FileInputStream("db.properties"));

            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            String driver = properties.getProperty("driver");
            String version="Not-Known";
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
            filename = request.getFilename();
            bucketname = request.getBucketname();
            S3Object s3Object = s3Client.getObject(new GetObjectRequest( bucketname, filename ));
//            S3Object s3Object = s3Client.getObject(new GetObjectRequest("termprojecttest", "result.csv"));
            InputStream objectData = s3Object.getObjectContent();
            //scanning data line by line
            Scanner scanner = new Scanner(objectData);
            String record;
            String header;
            String[] column_names;
            String[] values;
            Connection con = DriverManager.getConnection(url,username,password);
            header = scanner.nextLine();
            PreparedStatement ps = con.prepareStatement("SELECT table_name FROM information_schema.tables WHERE table_schema = 'term_project'");
            logger.log("Select query is " +ps);
            ResultSet rs = ps.executeQuery();
            if(!rs.next()) {

                column_names = header.split(",");
                String cmd = "create table SalesDB(";
                for (int i = 0; i < column_names.length-1; i++) {
                    column_names[i] = column_names[i].replaceAll(" ","_");
                    cmd = cmd + column_names[i] + " varchar(40)";
                    if ( column_names[i].equals("Order_ID")) {
                        cmd = cmd + " PRIMARY KEY";
                    }
                    cmd = cmd + ",";
                }
                cmd += (column_names[column_names.length - 1]).replaceAll(" ","_") + " varchar(40));";
                logger.log("query is " +cmd);
                ps.execute(cmd);
                logger.log("Created SalesDB table");
            }
            rs.close();
            Statement stmnt = con.createStatement();
            while (scanner.hasNext()) {
//                logger.log("scanned and ready for insert query");
                for (int bit = 0; bit < 5000 && scanner.hasNext(); bit++) {
                    record = scanner.nextLine();
                    values = record.split(",");
                    String cmd = "insert into SalesDB values ('";
                    for (int i = 0; i < values.length - 1; i++) {
                        cmd = cmd + values[i] + "','";
                        //logger.log("for loop");
                    }
//                logger.log("number in values array "+values.length);
                    cmd += values[values.length - 1] + "');";
                    logger.log("Insert query is" + cmd);
                    stmnt.addBatch(cmd);
                }
                stmnt.executeBatch();
            }
//                ps.execute(cmd);
                logger.log("Inserted into table");
        //rs.close();
            con.close();
        }
        catch (Exception e)
        {
            logger.log("Got an exception working with MySQL! ");
            logger.log(e.getMessage());
        }
        inspector.inspectAll();

        return inspector.RDSDetails(schemaname,tablename);
    }

        // int main enables testing function from cmd line
    public static void main (String[] args)
    {
        Context c = new Context() {
            @Override
            public String getAwsRequestId() {
                return "";
            }

            @Override
            public String getLogGroupName() {
                return "";
            }

            @Override
            public String getLogStreamName() {
                return "";
            }

            @Override
            public String getFunctionName() {
                return "";
            }

            @Override
            public String getFunctionVersion() {
                return "";
            }

            @Override
            public String getInvokedFunctionArn() {
                return "";
            }

            @Override
            public CognitoIdentity getIdentity() {
                return null;
            }

            @Override
            public ClientContext getClientContext() {
                return null;
            }

            @Override
            public int getRemainingTimeInMillis() {
                return 0;
            }

            @Override
            public int getMemoryLimitInMB() {
                return 0;
            }

            @Override
            public LambdaLogger getLogger() {
                return new LambdaLogger() {
                    @Override
                    public void log(String string) {
                        System.out.println("LOG:" + string);
                    }
                };
            }
        };

        // Create an instance of the class
        Load lt = new Load();

        

        Properties properties = new Properties();
        properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");
        properties.setProperty("url","");
        properties.setProperty("username","");
        properties.setProperty("password","");
        try
        {
            properties.store(new FileOutputStream("test.properties"),"");
        }
        catch (IOException ioe)
        {
            System.out.println("error creating properties file.")   ;
        }


        // Run the function
        //Response resp = lt.handleRequest(req, c);
        System.out.println("The MySQL Serverless can't be called directly without running on the same VPC as the RDS cluster.");
        Response resp = new Response();

        // Print out function result
        System.out.println("function result:" + resp.toString());
    }
}