package ordo;

import formats.Format.Type;

public interface JobInterfacePlus extends JobInterface {

	public void setInterFragFormat(Type ft);

	public void setInterFragFname(String fname);

	public void setInterFormat(Type ft);
	
	public void setInterFname(String fname);
	
	public void setOutputFormat(Type ft);
	
	public void setOutputFname(String fname);

}