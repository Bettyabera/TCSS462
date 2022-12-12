
package lambda;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import saaf.Inspector;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import static jdk.nashorn.internal.objects.Global.getDate;

/**
 *
 * @author betelhem & Kemeria
 * 
 */
public class TransferAndLoad implements RequestHandler<Request, HashMap<String, Object>> {
    
    String bucketname = "";
    String filename = "";
    int transferLoad= 0;
    String className = "TransferAndLoad";
    public HashMap<String, Object> handleRequest(Request request, Context context) {  
        
        
        //Collect inital data.
        Inspector inspector = new Inspector();        
        inspector.inspectAll();
        
        //****************START FUNCTION IMPLEMENTATION*************************   
        
        transferLoad = request.getService();
        
        switch (transferLoad) {
            case 1:
                bucketname = request.getBucketname();
                filename = request.getFilename();
                Transform(bucketname, filename);
                break;
            case 2:
                bucketname = request.getBucketname();
                filename = request.getFilename();
                Load(bucketname, filename,context);
             
                break;
        }
        //****************END FUNCTION IMPLEMENTATION***************************
                
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    
    
    
    
}
    public void transformCSVData(InputStream ipStream) throws IOException{
         
        final String comma=",";
        
        //ArrayList to hold each row of the csv file
        List<ArrayList<String>> rows = new ArrayList<>();
        
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(ipStream))) {
            String row = null;
            
            while ((row = reader.readLine()) != null) {
                String[] data = row.split(comma);
                
                ArrayList<String> rowList = new ArrayList<>();
                            
                //Add the each row of data to an Arraylist
                Collections.addAll(rowList, data);
                rows.add(rowList);
            }
        }
        
       
        rows = transformData(rows);     
        
        //Write the Transfomred file to S3
        writeCSV(rows);
    }  
    public void writeCSV(List<ArrayList<String>> rows){
        
        //Creating values for csv file        
        StringWriter sw = new StringWriter();
  
        for(List<String> row: rows) {
            int i = 0;
            for (String value: row) {
                sw.append(value);
                if(i++ != row.size() - 1)
                    sw.append(',');
            }
            sw.append("\n");            
        }
                 
        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
        InputStream is = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("text/plain");
        
        String testedFile= "test.csv";
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        s3Client.putObject(bucketname, testedFile, is, meta);
    }
    public List<ArrayList<String>> transformData(List<ArrayList<String>> rows){
   
        rows = priority(rows);    
      
        rows = grossMargin(rows);
        
        rows = removeDuplicates(rows);
        
        rows = processTime(rows);
        
        
        return rows;
    }
    /**
     * Transform Service 
     */
    public void Transform(String bucketname, String filename){
        
        
        Logger.getLogger(className+ ": In Transform function");
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        //get object file using source bucket and srcKey name
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        //get content of the file
        InputStream objectData = s3Object.getObjectContent();
        
        try {  
           
            transformCSVData(objectData);
        } catch (IOException ex) {
            Logger.getLogger(TransferAndLoad.class.getName()).log(Level.SEVERE, null, ex);
        } 
    }
    
     

    /**
     *
     * @param rows
     * @return Change the priority name to there initials 
     */
    public List<ArrayList<String>> priority(List<ArrayList<String>> rows){
        int priorityColum = 4;
        
        rows.forEach((ArrayList<String> iterator) -> {
         
            String priority = iterator.get(priorityColum);
            
          switch(priority){
              case "H":
                  iterator.set(priorityColum, "High");
                  break;
              case "M":
                  iterator.set(priorityColum, "Medium");
                  break;
              case "L":
                  iterator.set(priorityColum, "Low");
                  break;
              case "C":
                  iterator.set(priorityColum, "Critical");
                  break;
          }
        });
       
        return rows;
    }
    
  
    
   
    public List<ArrayList<String>> grossMargin(List<ArrayList<String>> rows){
        int revenuColum = 11;
        int profitColum = 13;
     
    
        for (int i= 0; i < rows.size(); i++){
            if(i==0){
                
                rows.get(i).add("Gross Margin") ;             
            }
            else{
                
                String revenue = rows.get(i).get(revenuColum);
                String profit = rows.get(i).get(profitColum);
                
                
                float margin = Float.parseFloat(profit) / Float.parseFloat(revenue);
                
                rows.get(i).add(String.format("%.3f", margin));                
            }
        }        
       
        return rows;
    }
    
    public List<ArrayList<String>> removeDuplicates(List<ArrayList<String>> rows){
        
        int orderIdCol=6;
        List<ArrayList<String>> newList = new ArrayList<>();
        rows.forEach((iterator) -> {
            
            String orderId = iterator.get(orderIdCol);
            
            boolean isDuplicate = false;
            
            for (List<String> newIterator : newList) {
                String newOrderId = newIterator.get(orderIdCol);
                if(orderId.equals(newOrderId)) {
                    isDuplicate = true;
                }
            }
            
            if (!isDuplicate) {
                newList.add(iterator);
            }
        });
        
        return newList;
    }
        
    public List<ArrayList<String>> processTime(List<ArrayList<String>> rows){
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");

        Date orderDate = null;
        Date shipDate = null;

        for (int i= 0; i < rows.size(); i++){
            if(i==0){
                //Adding a header in the first row
                rows.get(i).add("Order Processing Time") ;             
            }
            else{
                try {
                    //Get the orderdate and ship date
                    orderDate = sdf.parse(rows.get(i).get(5));
                    shipDate = sdf.parse(rows.get(i).get(7));
                } catch (ParseException ex) {
                    Logger.getLogger(Transform.class.getName()).log(Level.SEVERE, null, ex);
                }                
                
                //Calculate the number of days between order date and ship date
                long timeDifferenceInMilliSeconds = shipDate.getTime() - orderDate.getTime();
                long numOfDays = TimeUnit.MILLISECONDS.toDays(timeDifferenceInMilliSeconds);
                
                rows.get(i).add(String.valueOf(numOfDays));                
            }
        }        
        // return the Updated list
        return rows;
    }
    
    
   
    /**
     * Load Service
     * 
     */
   public void Load(String bucketname, String filename,Context context){
       
          LambdaLogger logger = context.getLogger();
       
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
        saaf.Response resp = new saaf.Response();

        // Print out function result
        System.out.println("function result:" + resp.toString());
    }
}