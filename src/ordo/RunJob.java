package ordo;

import java.rmi.RemoteException;

import formats.Format;
import map.Mapper;

public class RunJob extends Thread {

	private Daemon d;
	private Mapper mr;
	private Format reader;
	private Format writer;
	private CallBack cb;

	public RunJob(Daemon d, Mapper mr, Format reader, Format writer, CallBack cb) {
		this.d = d;
		this.mr = mr;
		this.reader = reader;
		this.writer = writer;
		this.cb = cb;
	}

	@Override
	public void run() {
		try {
			// System.out.println("start " + d.toString());
			d.runMap(mr, reader, writer, cb);
			// System.out.println("fin " + d.toString());
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

}
