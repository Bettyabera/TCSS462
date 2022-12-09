
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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import saaf.Inspector;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author betelhem
 */
public class TransferAndLoad implements RequestHandler<Request, HashMap<String, Object>> {
    
    String bucketname = "";
    String filename = "";
    int service = 0;
    public HashMap<String, Object> handleRequest(Request request, Context context) {  
        
        //Collect inital data.
        Inspector inspector = new Inspector();        
        inspector.inspectAll();
        
        //****************START FUNCTION IMPLEMENTATION*************************   
        
        service = request.getService();
        
        switch (service) {
            case 1:
                bucketname = request.getBucketname();
                filename = request.getFilename();
                Transform(bucketname, filename);
                break;
            case 2:
                bucketname = request.getBucketname();
                filename = request.getFilename();
                //Load(bucketname, filename);
                break;
        }
        //****************END FUNCTION IMPLEMENTATION***************************
                
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    
    
    
    
}
    public void transformCSVData(InputStream ipStream) throws IOException{
         
        final String lineSep=",";
        
        //ArrayList to hold each row of the csv file
        List<ArrayList<String>> rows = new ArrayList<>();
        
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(ipStream))) {
            String row = null;
            
            while ((row = reader.readLine()) != null) {
                String[] data = row.split(lineSep);
                
                ArrayList<String> rowList = new ArrayList<>();
                            
                //Add the each row of data to an Arraylist
                Collections.addAll(rowList, data);
                rows.add(rowList);
            }
        }
        
        // Do the 4 transformations on the original data
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
    public void Transform(String bucketname, String filename){
        
        String LOGGER_CLASSNAME = "TransferAndLoad";
        Logger.getLogger(LOGGER_CLASSNAME+ ": In Transform function");
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        //get object file using source bucket and srcKey name
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
        //get content of the file
        InputStream objectData = s3Object.getObjectContent();
        
        try {  
            //Do the transformations on the csv file and write as a new file to S3
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
    
   
    
   
   
}
