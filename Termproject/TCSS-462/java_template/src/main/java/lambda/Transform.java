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

import java.util.ArrayList;
import java.util.Collections;
import saaf.Inspector;
import java.util.HashMap;
import java.util.List;

import java.util.logging.Level;
import java.util.logging.Logger;


public class Transform implements RequestHandler<Request, HashMap<String, Object>> {
    
    String bucketname = "";
    String filename = ""; 
 
    /**
     * Lambda Function Handler
     * 
     * @param request Request POJO with defined variables from Request.java
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        
        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        
        //****************START FUNCTION IMPLEMENTATION*************************                
        bucketname = request.getBucketname();
        filename = request.getFilename();
        
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
  
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
  
        InputStream objectData = s3Object.getObjectContent();
        
        try {  
            
            transformCSVData(objectData);
        } catch (IOException ex) {
            Logger.getLogger(Transform.class.getName()).log(Level.SEVERE, null, ex);
        }         
                
        //****************END FUNCTION IMPLEMENTATION***************************
                
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
    
    public void transformCSVData(InputStream ipStream) throws IOException{
        BufferedReader csvReader=null;
        final String lineSep=",";
        
        //ArrayList to hold each row of the csv file
        List<ArrayList<String>> rows = new ArrayList<>();
        
        csvReader = new BufferedReader(new InputStreamReader(ipStream));
        String row = null;
        
        while ((row = csvReader.readLine()) != null) {
            String[] data = row.split(lineSep);
            
            ArrayList<String> rowList = new ArrayList<>();
            
            //Add the each row of data to an Arraylist
            Collections.addAll(rowList, data);
            rows.add(rowList);            
        }
        csvReader.close();
        
      
        rows = transformData(rows);     
        
        //Write the Transformed file to S3
        writeCSV(rows);
    }  
        
   
    public List<ArrayList<String>> transformData(List<ArrayList<String>> rows){
   
        rows = priority(rows);    
      
        rows = grossMargin(rows);
        
        
        return rows;
    }
    
     public List<ArrayList<String>> priority(List<ArrayList<String>> rows){
        int priorityColum = 4;
        
        rows.forEach((ArrayList<String> iterator) -> {
         
            String priority = iterator.get(priorityColum);
            
            switch (priority) {
                case "L":
                    iterator.set(priorityColum, "Low");
                    break;
                case "M":
                    iterator.set(priorityColum, "Medium");
                    break;
                case "H":
                    iterator.set(priorityColum, "High");
                    break;
                case "C":
                    iterator.set(priorityColum, "Critical");
                    break;
                default:
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
                //Adding a header in the first row
                rows.get(i).add("Gross Margin") ;             
            }
            else{
                //Get the revenue & Profit
                String revenue = rows.get(i).get(revenuColum);
                String profit = rows.get(i).get(profitColum);
                
                //Calculate margin
                float margin = Float.parseFloat(profit) / Float.parseFloat(revenue);
                //Add the margin to the csv file
                rows.get(i).add(String.format("%.2f", margin));                
            }
        }        
        // return the Updated list
        return rows;
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
        
        // Create new file on S3
        String testedFile= "test.csv";
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        s3Client.putObject(bucketname, testedFile, is, meta);
    }
}