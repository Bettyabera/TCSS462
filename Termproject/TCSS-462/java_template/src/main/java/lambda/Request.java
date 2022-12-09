package lambda;

/**
 *
 * @author Wes Lloyd
 */
public class Request {

    String name;
    private String bucketname;
    private String filename;
    private int service;

    public String getName() {
        return name;
    }
    
    public String getNameALLCAPS() {
        return name.toUpperCase();
    }

    public void setName(String name) {
        this.name = name;
    }

    public Request(String name) {
        this.name = name;
    }

    public Request() {

    }

    /**
     * @return the bucketname
     */
    public String getBucketname() {
        return bucketname;
    }

    /**
     * @param bucketname the bucketname to set
     */
    public void setBucketname(String bucketname) {
        this.bucketname = bucketname;
    }

    /**
     * @return the filename
     */
    public String getFilename() {
        return filename;
    }

    /**
     * @param filename the filename to set
     */
    public void setFilename(String filename) {
        this.filename = filename;
    }

    /**
     * @return the service
     */
    public int getService() {
        return service;
    }

    /**
     * @param service the service to set
     */
    public void setService(int service) {
        this.service = service;
    }
}
