
package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
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
                Load(bucketname, filename);
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
     */
   public void Load(String bucketname, String filename){
       
        Logger.getLogger(className+ ": In Load function");
        try 
        {
            Properties properties = new Properties();
            properties.load(new FileInputStream("db.properties"));
            
            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            String driver = properties.getProperty("driver");
            
            Connection con = DriverManager.getConnection(url,username,password);
            
            // Detect if the table 'Sales_Data' exists in the database
            PreparedStatement ps = con.prepareStatement("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'SALESDB' AND TABLE_NAME = 'Sales_Data'");            
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                Logger.getLogger("Create table 'Sales_Data'");
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
            String sqlQuery = "INSERT INTO Sales_Data VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            
            while (scanner.hasNext()) {
                if(skip) {
                    skip = false; // Skip the first line - headers
                    record = scanner.nextLine();
                    continue;
                }
                
                record = scanner.nextLine();
                values = record.split(",");               
                
                ps = con.prepareStatement(sqlQuery);
                // Example Insert - Australia and Oceania,Tuvalu,Baby Food,Offline,High,
                //5/28/2010,669165933,6/27/2010,9925,255.28,159.42,
                //2533654,1582243.5,951410.5,0.38,30
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
            Logger.getLogger("SQL Exception:" + sqlex.toString());
            Logger.getLogger(sqlex.getMessage());
        }catch (Exception ex) {
            Logger.getLogger("Got an exception working with MySQL!" + ex.toString());
            Logger.getLogger(ex.getMessage());
        }        
    }
    private Integer getInt(String integer) {
        return Integer.valueOf(integer);
    }

    private Double getDouble(String doubleVal) {
        return Double.valueOf(doubleVal);
}
     private java.sql.Date getDate(String date) throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yy");
        return new java.sql.Date(formatter.parse(date).getTime());
    }
}