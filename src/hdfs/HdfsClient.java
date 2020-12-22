package hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import config.Settings;
import formats.Format;
import formats.KV;

public class HdfsClient {

	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <line|kv> <file>");
		System.out.println("Usage: boolean isDone = false;java HdfsClient delete <file>");
	}

	public static void HdfsDelete(String hdfsFname) {

		try {
			// loadConfig(config_path);
			Registry registry = LocateRegistry.getRegistry(Settings.HDFS_HOST, Settings.HDFS_PORT);
			NameNode nameNode = (NameNode) registry.lookup(Settings.HDFS_NAME);
			MetaDataFichier metadata = nameNode.GetMetadataFile(hdfsFname);

			List<Chunks> chunksList = metadata.getChunks();

			for (Chunks chunk : chunksList) {
				List<DataNode> dataNodeList = chunk.getDatanodes();
				String ip = dataNodeList.get(0).getIp();
				int port = dataNodeList.get(0).getPort();

				Socket socket = new Socket(ip, port);
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());

				Commande commande = new Commande(Commande.Id.Commande_Delete, chunk.getName()/* on doit avoir l */,
						metadata.getFormat());

				objectOutputStream.writeObject(commande);
				objectOutputStream.close();

			}
			nameNode.supprimerMetaDataFichier(metadata.getNomFich());

		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String debug() {
		String ans = "";
		try {
			System.out.println("rmi access : " + Settings.HDFS_HOST + ":" + Settings.HDFS_PORT);
			ans += "//" + Settings.HDFS_HOST + ":" + Settings.HDFS_PORT + "/" + Settings.HDFS_NAME + "\n";
			NameNode nameNode = (NameNode) Naming
					.lookup("//" + Settings.HDFS_HOST + ":" + Settings.HDFS_PORT + "/" + Settings.HDFS_NAME);
			ans += "1\n";
			ans += "\n";
			for (DataNode dn : nameNode.listeDataNodes())
				ans += dn.toString() + "\n";
			List<DataNode> dataNodeList = nameNode.listeDataNodes();
		} catch (RemoteException e) {
			ans += "RemoteException " + e.getMessage();
		} catch (NotBoundException e) {
			ans += "NotBoundException " + e.getMessage();
		} catch (MalformedURLException e) {
			ans += "AlreadyBoundException " + e.getMessage();
		}
		return ans;
	}

	public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor) {
		long fragment_size = Settings.FRAGMENT_SIZE;
		long readed = 0, size = 0L;
		boolean isDone = false;
		try {
			if (Settings.DEBUG) {
				System.out.println("Client Ecrit");
				System.out.println("rmi access : " + Settings.HDFS_HOST + ":" + Settings.HDFS_PORT);
			}
			Registry registry = LocateRegistry.getRegistry(Settings.HDFS_HOST, Settings.HDFS_PORT);
			NameNode nameNode = (NameNode) registry.lookup(Settings.HDFS_NAME);
			List<DataNode> dataNodeList = nameNode.listeDataNodes();
			if (Settings.DEBUG) {
				System.out.println("Datanode list size : " + dataNodeList.size());
				System.out.println(dataNodeList.get(0).getIp() + ":" + dataNodeList.get(0).getPort() + ":"
						+ dataNodeList.get(0).getName());
			}
			File file = new File(localFSSourceFname);
			// Format format = null ;
			// switch(fmt)
			// {
			// case KV:
			// format = new KVFormat(file.getName());
			// System.out.println("KV Format");
			// break;
			// case LINE:
			// format = new LineFormat();
			// System.out.println("Line Format");
			// break;
			// }
			// format.setFname(localFSSourceFname);
			Format format = Format.get(localFSSourceFname, fmt);
			format.open(Format.OpenMode.R);
			/// On cree le metadatafile

			MetaDataFichier metaDataFichier = new MetaDataFichier(localFSSourceFname, file.length(), fmt);
			List<Chunks> chunksList = new ArrayList<Chunks>();

			if (Settings.DEBUG) {
				System.out.println(
						" Fichier existe ? : " + file.exists() + " " + localFSSourceFname + " " + file.length());
				System.out.println("Attempting to send data to the server");
			}

			String[] path = localFSSourceFname.split("/");
			String fragName = path[path.length - 1];
			int index_dot = fragName.indexOf('.');
			String fragNamedeb = fragName.substring(0, index_dot);
			String ext = fragName.substring(index_dot);

			int num_frag = 0;
			Random rnd = new Random();
			int i;
			while (!isDone) {
				i = rnd.nextInt(dataNodeList.size());
				// Ici la commande de l'ecriture
				if (Settings.DEBUG)
					System.out.println("creating the command");

				Commande commande = new Commande(Commande.Id.Commande_WRITE,
						Settings.TMP_PATH + fragNamedeb + "_" + num_frag + ext, fmt);

				// envoie maint par les sockets
				List<Socket> socketListClient = new ArrayList<>();
				// Creation des outputstreams pour l'envoi

				List<ObjectOutputStream> objectOutputStreamList = new ArrayList<ObjectOutputStream>();
				for (int k = 0; k < repFactor; k++) {
					readed = 0L;
					if (Settings.DEBUG)
						System.out.println("get the ip:port from the datanode");
					int suivant = (i + k) % dataNodeList.size();
					String ip = dataNodeList.get(suivant).getIp();
					int port = dataNodeList.get(suivant).getPort();
					if (Settings.DEBUG) {
						System.out.println(ip + ":" + port);
						System.out.println("create the socket and adding it to the socket list");
					}
					Socket socket = new Socket(ip, port);
					socketListClient.add(socket);
					if (Settings.DEBUG)
						System.out.println("getting the object output stream via the  socket");
					objectOutputStreamList.add(new ObjectOutputStream(socketListClient.get(k).getOutputStream()));
					if (Settings.DEBUG) {
						System.out
								.println(commande.getCmd() + ":" + commande.getNomChunk() + ":" + commande.getformat());
						System.out.println("send the object to the server");
					}
					objectOutputStreamList.get(k).writeObject(commande);
					objectOutputStreamList.get(k).reset();
				}
				while (readed < Settings.FRAGMENT_SIZE) {
					KV kv = format.read();
					if (kv == null) {
						isDone = true;
						break;
					}
					// in java, length of a char is 2 bytes
					readed += kv.v.length() * 2 + kv.k.length() * 2;
					if (Settings.DEBUG) {
						System.out.println("Readed : " + readed);
						System.out.println(kv.toString());
					}

					for (int j = 0; j < repFactor; j++) {
						objectOutputStreamList.get(j).writeObject(kv);

						objectOutputStreamList.get(j).reset();
					}
					// if (i == dataNodeList.size() && !isDone)
					// i = 0;
				}

				try {
					Thread.sleep(400);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				// finalisation et ajout de metadatachunk et le dupliquer
				if (Settings.DEBUG)
					System.out.println("creating chunks and closing connection");
				Chunks chunk = new Chunks(Settings.TMP_PATH + fragNamedeb + "_" + num_frag + ext, readed, repFactor);
				for (int k = 0; k < repFactor; k++) {
					objectOutputStreamList.get(k).writeObject(null);
					int suivant = (i + k) % dataNodeList.size();
					DataNode dataNode = dataNodeList.get(suivant);
					if (Settings.DEBUG) {
						System.out.println("dataNodeList: " + dataNodeList);
						System.out.println("suivant: " + suivant);
						System.out.println("dataNode: " + dataNode);
					}
					chunk.addDatanode(dataNode);
					objectOutputStreamList.get(k).close();
					socketListClient.get(k).close();
				}

				chunksList.add(chunk);
				if (isDone)
					break;
				num_frag++;
			}

			format.close();

			metaDataFichier.setChunks(chunksList);
			nameNode.addMetaDataFichier(metaDataFichier);
			;
		} catch (RemoteException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (NotBoundException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	public static void HdfsRead(String hdfsFname, String localFSDestFname) {

		ObjectOutputStream objectOutputStream = null;
		ObjectInputStream objectInputStream = null;
		Socket clientSocket = null;

		try {
			Registry registry = LocateRegistry.getRegistry(Settings.HDFS_HOST, Settings.HDFS_PORT);

			NameNode nameNode = (NameNode) registry.lookup(Settings.HDFS_NAME);
			String[] path = hdfsFname.split("/");
			String fileName = path[path.length - 1];
			MetaDataFichier metaDataFichier;
			String deb_path = String.join("/", Arrays.copyOf(path, path.length - 1)) + "/";
			String ext = "";
			Format.Type format_f;

			// if (fileName.startsWith(Settings.MOTIF)) {
			if (fileName.endsWith(".res")) {
				// deb_path += Settings.MOTIF;
				// metaDataFichier = nameNode.GetMetadataFile(Settings.DATA_PATH +
				// fileName.replace(Settings.MOTIF, ""));
				metaDataFichier = nameNode
						.GetMetadataFile(Settings.DATA_PATH + fileName.substring(0, fileName.length() - 4));
				format_f = Format.Type.KV;
				ext = ".res";
			} else {
				metaDataFichier = nameNode.GetMetadataFile(hdfsFname);
				format_f = metaDataFichier.getFormat();
			}
			Format fmt = Format.get(localFSDestFname, format_f);
			fmt.open(Format.OpenMode.W);

			List<Chunks> chunks = metaDataFichier.getChunks();
			for (Chunks chunk : chunks) {

				List<DataNode> dataNodeList = chunk.getDatanodes();
				for (int i = 0; i < chunk.getDatanodes().size(); i++) {
					try {
						KV enregistrement = null;
						String[] _path = chunk.getName().split("/");
						String name = deb_path + _path[_path.length - 1] + ext;
						clientSocket = new Socket(dataNodeList.get(i).getIp(), dataNodeList.get(i).getPort());
						Commande commande = new Commande(Commande.Id.Commande_Read, name, format_f);
						objectOutputStream = new ObjectOutputStream(clientSocket.getOutputStream());
						objectOutputStream.writeObject(commande);
						objectInputStream = new ObjectInputStream(clientSocket.getInputStream());
						// System.out.println("first read: " + objectInputStream.readObject());
						while ((enregistrement = (KV) objectInputStream.readObject()) != null) {
							if (Settings.DEBUG)
								System.out.println("CLIENT GET:" + enregistrement);
							fmt.write(enregistrement);
							if (Settings.DEBUG)
								System.out.println("UPDATE fmt: " + fmt);
						}
						break;
					} catch (IOException e) {
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					} finally {
						try {
							if (objectInputStream != null)
								objectInputStream.close();
							if (objectOutputStream != null)
								objectOutputStream.close();
							if (clientSocket != null)
								clientSocket.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

				}
			}
			fmt.close();

		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		// java HdfsClient <read|write> <line|kv> <file>

		try {
			if (args.length < 2) {
				usage();
				return;
			}

			switch (args[0]) {
			case "read":
				HdfsRead(args[1], args[2]);
				break;
			case "delete":
				HdfsDelete(args[1]);
				break;
			case "write":
				Format.Type fmt;
				if (args.length < 3) {
					usage();
					return;
				}
				if (args[1].equals("line"))
					fmt = Format.Type.LINE;
				else if (args[1].equals("kv"))
					fmt = Format.Type.KV;
				else {
					usage();
					return;
				}
				HdfsWrite(fmt, args[2], 1);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
