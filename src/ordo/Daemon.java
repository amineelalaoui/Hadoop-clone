package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

import formats.Format;
import map.Mapper;

public interface Daemon extends Remote {
	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException;

	// TODO
	public void setInputFormat(Format.Type ft) throws RemoteException;

	public void setInputFname(String fname) throws RemoteException;

	public void setOutputFormat(Format.Type ft) throws RemoteException;

	public void setOutputFname(String fname) throws RemoteException;

	void runMap(Mapper m, CallBack cb) throws RemoteException;
}
