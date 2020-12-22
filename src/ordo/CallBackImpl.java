package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Semaphore;

import config.Settings;

@SuppressWarnings("serial")
public class CallBackImpl extends UnicastRemoteObject implements CallBack {

	private Semaphore done;
	private Semaphore lock;
	private String hostname;

	private Iterator<String> iterator;
	private int todo;

	public CallBackImpl(String host, List<String> todo) throws RemoteException {
		super();
		this.hostname = host;
		this.done = new Semaphore(0);
		this.lock = new Semaphore(1);

		this.todo = todo.size();
		this.iterator = todo.iterator();
	}

	// TODO fusionner le end() et le next() pour éviter des problèmes de concurrence

	@Override
	public boolean end() throws RemoteException {
		try {
			this.lock.acquire();
			if (!iterator.hasNext()) {
				this.endMap(); // Comment garantir qu'il ne reste pas des Thread qui calcul encore
				return true;
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public String next() throws RemoteException {
		String ans = null;
		ans = this.iterator.next();
		this.lock.release();
		return ans;
	}

	@Override
	public void endMap() throws RemoteException {
		this.done.release();
		if (Settings.DEBUG)
			System.out.println("end runMap " + this.getHostName());
	}

	@Override
	public void isFinished() throws RemoteException, InterruptedException {
		this.done.acquire();
	}

	@Override
	public String getHostName() throws RemoteException {
		return this.hostname;
	}

	@Override
	public int nbs() throws RemoteException {
		return this.todo;
	}

}
