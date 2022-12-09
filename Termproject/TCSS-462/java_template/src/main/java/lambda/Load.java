
package lambda;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import saaf.Inspector;
import saaf.Response;
import java.util.HashMap;
import java.util.Scanner;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Date;


/**
 *
 */
public class Load implements RequestHandler<Request, HashMap<String, Object>>{
   
     /**
     * Lambda Function Handler
     *
     * @param request Request POJO with defined variables from Request.java
     * @param context
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context){
       

   

        // Create logger
        LambdaLogger logger = context.getLogger();        

        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();
       
        //****************START FUNCTION IMPLEMENTATION*************************        
        String bucketname = request.getBucketname();
        String filename = request.getFilename();
       

        try
        {
            Properties properties = new Properties();
            properties.load(new FileInputStream("db.properties"));
           
            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            String driver = properties.getProperty("driver");
           
            Connection con = DriverManager.getConnection(url,username,password);
           
            // Check if the table 'Sales_Data' exists in the database
            PreparedStatement ps = con.prepareStatement("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'SALESDB' AND TABLE_NAME = 'Sales_Data'");            
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                // 'Sales_Data' does not exist, and should be created
                logger.log("Create table 'Sales_Data'");
                ps = con.prepareStatement("CREATE TABLE Sales_Data ("
                        + " Region varchar(255),"
                        + " Country varchar(255),"
                        + " Item Type varchar(255),"
                        + " Sales Channel varchar(255),"
                        + " Order Priority varchar(255),"
                        + " Order Date DATE,"
                        + " Ship_Date DATE,"
                        + " Units_Sold INT,"
                        + " Unit_Price DOUBLE,"
                        + " Unit_Cost DOUBLE,"
                        + " Total_Revenue DOUBLE,"
                        + " Total_Cost DOUBLE,"
                        + " Total_Profit DOUBLE,"
                        + " Gross_Margin DOUBLE,"
                        + " Order Process Time INT,"
                        + " PRIMARY KEY(Order_ID));");
                ps.execute();
            }
            rs.close();
             // Delete all rows from table - "Sales_Data" before inserting new rows.
            ps = con.prepareStatement("Delete from Sales_Data;");
            ps.execute();
           
            // Reading the csv file from the S3 Bucket
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
            //get object file using source bucket and srcKey name
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
                       
            //get content of the file
            InputStream objectData = s3Object.getObjectContent();
            Scanner scanner = new Scanner(objectData);
           
            boolean skip=true;
            String record = "";
            String[] values;
            String sqlQuery = "INSERT INTO Sales_Data VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
           
            while (scanner.hasNext()) {
                if(skip) {
                    skip = false;
                    record = scanner.nextLine();
                    continue;
                }
               
                record = scanner.nextLine();
                values = record.split(",");              
               
                ps = con.prepareStatement(sqlQuery);
               
                ps.setString(1, values[0]);
                ps.setString(2, values[1]);
                ps.setString(3, values[2]);
                ps.setString(4, values[3]);
                ps.setString(5, values[4]);
                ps.setDate(6, getDate(values[5]));
                ps.setInt(7, getInt(values[6]));
                ps.setDate(8, getDate(values[7]));
                ps.setInt(9, getInt(values[8]));
                ps.setDouble(10, getDouble(values[9]));
                ps.setDouble(11, getDouble(values[10]));
                ps.setDouble(12, getDouble(values[11]));
                ps.setDouble(13, getDouble(values[12]));
                ps.setDouble(14, getDouble(values[13]));
                ps.setDouble(15, getDouble(values[14]));
                ps.setInt(16, getInt(values[15]));
               
                ps.executeUpdate();
                ps.close();          
            }
            scanner.close();        
           
            con.close();                      
        } catch (SQLException sqlex) {
            logger.log("SQL Exception:" + sqlex.toString());
            logger.log(sqlex.getMessage());
        }catch (Exception ex) {
            logger.log("Got an exception working with MySQL!" + ex.toString());
            logger.log(ex.getMessage());
           
           
        }        
       
         //****************END FUNCTION IMPLEMENTATION***************************
       
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
   
   
     private Integer getInt(String integer) {
        return Integer.valueOf(integer);
    }

    private Double getDouble(String doubleVal) {
        return Double.valueOf(doubleVal);
    }

    private Date getDate(String date) throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yy");
        return new java.sql.Date(formatter.parse(date).getTime());
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
        Load ld = new Load();

        // Create a request object
        Request req = new Request();

       
        String bucketName = (args.length > 0 ? args[0] : "");
        String fileName = (args.length > 0 ? args[1] : "");

       
        req.setBucketname(bucketName);
        req.setFilename(fileName);  
       
       
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
        System.out.println("The MySQL Serverless can't be called directly without running on the same VPC as the RDS cluster.");
 
        Response resp = new Response();

        // Print out function result
        System.out.println("function result:" + resp.toString());
    }



}