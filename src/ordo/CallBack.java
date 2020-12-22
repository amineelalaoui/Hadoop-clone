package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CallBack extends Remote {

	public void endMap() throws RemoteException;

	public void isFinished() throws RemoteException, InterruptedException;

	public String getHostName() throws RemoteException;

	// TODO Change names
	public int nbs() throws RemoteException; // Vraiment utile (?)

	public boolean end() throws RemoteException;

	public String next() throws RemoteException;

}
