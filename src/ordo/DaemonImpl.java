package ordo;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import config.Settings;
import formats.Format;
import formats.Format.OpenMode;
import formats.Format.Type;
import map.Mapper;

public class DaemonImpl extends UnicastRemoteObject implements Daemon, Runnable {

	private static final long serialVersionUID = 1L;

	private Type inputFormat;
	private String inputFname;
	private Type outputFormat;
	private String outputFname;

	private Mapper mapper;
	private CallBack cb;

	public DaemonImpl() throws RemoteException {
		super();
	}

	@Override
	public void setInputFormat(Type ft) throws RemoteException {
		this.inputFormat = ft;
	}

	@Override
	public void setInputFname(String fname) throws RemoteException {
		this.inputFname = fname;
	}

	@Override
	public void setOutputFormat(Type ft) throws RemoteException {
		this.outputFormat = ft;
	}

	@Override
	public void setOutputFname(String fname) throws RemoteException {
		this.outputFname = fname;
	}

	@Override
	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
	}

	@Override
	public void runMap(Mapper m, CallBack cb) throws RemoteException {
		this.mapper = m;
		this.cb = cb;
		int maxThread = Integer.min(Runtime.getRuntime().availableProcessors(), cb.nbs());
		for (int c = 0; c < maxThread; c++) {
			new Thread(this).start();
		}
	}

	@Override
	public void run() {
		try {
			while (!this.cb.end()) {
				String filename = this.cb.next();
				Format reader = Format.get(filename, this.inputFormat);
				reader.open(OpenMode.R);
				// Format writer = Format.get(getOutputFname(filename), this.outputFormat);
				Format writer = Format.get(filename + ".res", outputFormat);
				writer.open(OpenMode.W);
				if (Settings.DEBUG)
					System.out.println("Start calculs of " + filename + " on " + this.cb.getHostName()
							+ Thread.currentThread().getName().substring(6));
				this.mapper.map(reader, writer);
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	public String getOutputFname(String filename) {
		String[] split = filename.split("_");
		return this.outputFname + "_" + split[split.length - 1];
	}

	public static void main(String args[]) {
		if (Settings.DEBUG)
			System.out.println(Settings.HIDOOP_DAEMON_NAME + " launch");
		try {
			Daemon d = new DaemonImpl();
			Registry registry = LocateRegistry.createRegistry(Settings.HIDOOP_PORT);
			registry.bind(Settings.HIDOOP_DAEMON_NAME, d);
		} catch (Exception e) {
			System.out.println(e);
		}
	}

}
