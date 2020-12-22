package ordo;

import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import config.Settings;
import formats.Format;
import formats.Format.OpenMode;
import formats.Format.Type;
import hdfs.HdfsClient;
import hdfs.NameNode;
import map.MapReduce;

public class Job implements JobInterfacePlus {

	private Type inputFormat;
	private String inputFname;

	private Type interFragFormat;
	private String interFragFname;

	private Type interFormat;
	private String interFname;

	private Type outputFormat;
	private String outputFname;

	public Job() {
		// Valeur par défaut si non redéfini
		this.interFragFormat = Type.KV;
		this.interFormat = Type.KV;
		this.outputFormat = Type.KV;
	}

	@Override
	public void setInputFormat(Type ft) {
		this.inputFormat = ft;
	}

	@Override
	public void setInputFname(String fname) {
		this.inputFname = fname;
		String[] path = fname.split("/");
		String name = path[path.length - 1];
		int index_dot = name.indexOf('.');
		String _name = name.substring(0, index_dot);
		String ext = name.substring(index_dot);
		// Si non défini, on met des noms par défaut
		if (this.interFragFname == null)
			this.setInterFragFname(Settings.TMP_PATH + _name + ext + Settings.MOTIF);
		if (this.interFname == null)
			this.setInterFname(Settings.TMP_PATH + _name + ext + Settings.MOTIF);
		if (this.outputFname == null)
			this.setOutputFname(Settings.DATA_PATH + _name + ext + Settings.MOTIF);
	}

	@Override
	public void setInterFragFormat(Type ft) {
		this.interFragFormat = ft;
	}

	@Override
	public void setInterFragFname(String fname) {
		this.interFragFname = fname;
	}

	@Override
	public void setInterFormat(Type ft) {
		this.interFormat = ft;
	}

	@Override
	public void setInterFname(String fname) {
		this.interFname = fname;
	}

	@Override
	public void setOutputFormat(Type ft) {
		this.outputFormat = ft;
	}

	@Override
	public void setOutputFname(String fname) {
		this.outputFname = fname;
	}

//	private Format get(String filename, Type type) {
//		if (type == Type.LINE) {
//			return new LineFormat(filename);
//		} else {
//			return new KVFormat(filename);
//		}
//	}

	public String getInterFragFnameOf(String filename) {
		String[] split = filename.split("_");
		return interFragFname + "_" + split[split.length - 1];
	}

	@Override
	public void startJob(MapReduce mr) {
		long ti = System.currentTimeMillis();
		Map<String, List<String>> mapping = new HashMap<>();
		try {
			Registry registry = LocateRegistry.getRegistry(Settings.HDFS_HOST, Settings.HDFS_PORT);
			NameNode nameNode = (NameNode) registry.lookup(Settings.HDFS_NAME);
			mapping = nameNode.getHostsByFiles();
		} catch (RemoteException | NotBoundException e1) {
			e1.printStackTrace();
		}

		// Lancement des CallBack sur chaque machine
		ArrayList<CallBack> cbs = new ArrayList<CallBack>();
		for (String host : mapping.keySet()) {
			try {
				if (Settings.DEBUG)
					System.out.println("lookup Daemon " + host + " and => run " + mapping.get(host));
				Daemon d = (Daemon) Naming
						.lookup("//" + host + ":" + Settings.HIDOOP_PORT + "/" + Settings.HIDOOP_DAEMON_NAME);
				d.setInputFormat(this.inputFormat);
				d.setInputFname(this.inputFname);
				d.setOutputFormat(this.interFragFormat);
				d.setOutputFname(this.interFragFname);

				CallBack cb = new CallBackImpl(host, mapping.get(host));
				cbs.add(cb);
				if (Settings.DEBUG)
					System.out.println("start runMap " + host);
				d.runMap(mr, cb);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		for (CallBack cb : cbs) {
			try {
				cb.isFinished();
			} catch (InterruptedException | RemoteException e) {
				e.printStackTrace();
			}
		}
		if (Settings.DEBUG)
			System.out.println("FIN ALL MAP!");
		System.out.println("Temps MAP: " + (System.currentTimeMillis() - ti));
		ti = System.currentTimeMillis();
		HdfsClient.HdfsRead(this.interFragFname, this.interFname);
		System.out.println("Temps HDFS-READ: " + (System.currentTimeMillis() - ti));

		// Lancement du reduce
		ti = System.currentTimeMillis();
		Format reader = Format.get(this.interFname, this.interFormat);
		reader.open(OpenMode.R);
		Format writer = Format.get(this.outputFname, this.outputFormat);
		writer.open(OpenMode.W);

		mr.reduce(reader, writer);
		System.out.println("Temps REDUCE: " + (System.currentTimeMillis() - ti));
		System.out.println("Le résultat se trouve dans " + this.outputFname);
	}

}
