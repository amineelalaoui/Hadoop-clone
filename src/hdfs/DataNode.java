package hdfs;

import java.io.Serializable;

/**
 * Created by Laaaag on 02/12/2019.
 */
public class DataNode implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = -6566386963849624737L;

	private String name ;
    private String ip ;
    private int port ;

    public DataNode(String name, String ip, int port) {
        super();
        this.name = name;
        this.ip = ip;
        this.port = port;
    }

    public DataNode(String ip, int port) {
        super();
        this.ip = ip;
        this.port = port;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
    
    @Override
    public String toString() {
    	
    	return ip+":"+port;
    }
}
