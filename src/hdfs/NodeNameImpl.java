package hdfs;

import java.io.File;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import config.Settings;
import formats.Format;
import formats.KV;
import formats.LineFormat;

/**
 * Created by EL ALAOUI on 02/12/2019
 */

public class NodeNameImpl extends UnicastRemoteObject implements NameNode {

	static private int port;
	private List<DataNode> dataNodes;
	private List<DataNode> daemons;
	private Map<String, List<String>> filesByHost;

	private String metaDataPath = Settings.TMP_PATH;

	protected NodeNameImpl() throws RemoteException {
		super();
		dataNodes = new ArrayList<DataNode>();
		daemons = new ArrayList<DataNode>();
		filesByHost = new HashMap<>();

	}

	public static void main(String args[]) {
		try {
			NameNode nameNode = new NodeNameImpl();
			// Registry registry = LocateRegistry.createRegistry(port);
			// registry.bind("nameNodeDaemon", nameNode);
			LocateRegistry.createRegistry(Settings.HDFS_PORT);
			Naming.rebind("rmi://localhost:" + Settings.HDFS_PORT + "/" + Settings.HDFS_NAME, nameNode);
			if (Settings.DEBUG)
				System.out.println("NodeName OK");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void configurationNameNode(String path) {

	}

	public MetaDataFichier GetMetadataFile(String nomFich) throws RemoteException {
		// TODO : add Format first

		// Creation de type Format ... et le Metadata
		if (Settings.DEBUG)
			System.out.println(nomFich);
		Format lineformat = new LineFormat(nomFich + "i");

		lineformat.open(Format.OpenMode.R);

		MetaDataFichier metaData = new MetaDataFichier();

		// informations du fichier et les chunks

		KV kv = lineformat.read();
		// information du fichier
		String[] informations = kv.v.split(":");
		metaData.setNomFich(informations[0]);
		if (Settings.DEBUG) {
			System.out.println(informations[0]);
			System.out.println(informations[1]);
		}
		metaData.setTaille(Long.parseLong(informations[1]));
		metaData.setFormat(Format.Type.valueOf(informations[2]));

		// informations du chunks

		List<Chunks> chunksList = new ArrayList<Chunks>();

		while ((kv = lineformat.read()) != null) {
			String[] informationsChunk = kv.v.split(":");

			Chunks chunk = new Chunks(informationsChunk[0], Long.parseLong(informationsChunk[1]),
					Integer.parseInt(informationsChunk[2]));

			for (int i = 0; i < Integer.parseInt(informationsChunk[2]); i++) {
				chunk.addDatanode(
						new DataNode((informationsChunk[3 + 2 * i]), Integer.parseInt(informationsChunk[4 + 2 * i])));
			}
			if (Settings.DEBUG)
				System.out.println("chunk ::::::::" + chunk);
			chunksList.add(chunk);

		}
		lineformat.close();
		metaData.setChunks(chunksList);
		return metaData;

	}

	public void addMetaDataFichier(MetaDataFichier fich) throws RemoteException {
		// TODO : add Format first

		Format format = new LineFormat(fich.getNomFich() + "i");
		format.open(Format.OpenMode.W);

		KV kv = new KV();

		// lire les information du fichier (MetaData à ajouter et les chunks )

		kv.v = fich.toString();
		format.write(kv);
		for (Chunks chunk : fich.getChunks()) {
			String fileName = chunk.getName();
			kv.v = chunk.toString();
			if (Settings.DEBUG)
				System.out.println(kv);
			format.write(kv);
			for (DataNode node : chunk.getDatanodes()) {
				List<String> files;
				if (Settings.DEBUG)
					System.out.println("file " + fileName + " is in " + node.getIp());
				if (filesByHost.containsKey(node.getIp())) {
					files = filesByHost.get(node.getIp());
				} else {
					filesByHost.put(node.getIp(), new ArrayList<>());
					files = filesByHost.get(node.getIp());
				}
				files.add(fileName);
			}
		}
		format.close();
	}

	public Map<String, List<String>> getHostsByFiles() {
		return filesByHost;
	}

	public void supprimerMetaDataFichier(String nomFich) throws RemoteException {
		File file = new File(nomFich);
		file.delete();
	}

	public List<DataNode> listeDataNodes() throws RemoteException {
		return dataNodes;
	}

	public void addDataNode(DataNode inf) throws RemoteException {
		dataNodes.add(inf);
	}

	@Override
	public String toString() {
		String ans = "ok    ";
		for (DataNode dn : this.dataNodes) {
			ans += dn.toString() + "\n";
		}
		return ans;
	}

}