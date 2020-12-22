package hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import config.Settings;
import formats.Format;
import formats.KV;

public class HdfsServer extends Thread {

	private Socket clientSocket;

	public void setClientSocket(Socket clientSocket) {
		this.clientSocket = clientSocket;
	}

	public Socket getClientSocket() {
		return clientSocket;
	}

	private void deleteHDFS(String chunk) {
		System.out.println("suppression du " + chunk);
		File file = new File(chunk);
		file.delete();
	}

	public void writeHDFS(Commande cmd, ObjectInputStream inputStream) {

		// Format format = Format.getFormatByType(cmd.getformat());
		// format.setFname(cmd.getNomChunk());
		Format format = Format.get(cmd.getNomChunk(), cmd.getformat());
		format.open(Format.OpenMode.W);
		KV enregistrement = null;
		try {

			while (true) {
				enregistrement = (KV) inputStream.readObject();
				if (enregistrement == null)
					break;
				if (Settings.DEBUG)
					System.out.println("readed  : " + enregistrement.k + ":" + enregistrement.v);
				format.write(enregistrement);
			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			format.close();
		}
	}

	private void readHDFS(Commande cmd, ObjectOutputStream outputStream) {
		// Format format = Format.getFormatByType(cmd.getformat());
		// format.setFname(cmd.getNomChunk());
		if (Settings.DEBUG)
			System.out.println("server hdfs read file: " + cmd.getNomChunk());
		Format format = Format.get(cmd.getNomChunk(), cmd.getformat());
		format.open(Format.OpenMode.R);
		KV enregistrement = null;
		boolean flag = false;
		try {
			do {
				enregistrement = format.read();
				if (enregistrement == null)
					flag = false;
				else
					flag = true;
				if (flag) {
					outputStream.writeObject(enregistrement);
					outputStream.reset();
				}
			} while (flag);
			outputStream.writeObject(null);
			outputStream.close();
		} catch (FileNotFoundException e) {
			System.out.println("Fichie non trouvé");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		format.close();
	}

	@Override
	public void run() {
		try {
			if (Settings.DEBUG) {
				System.out.println("Current thread " + Thread.currentThread().getId());
				System.out.println("Connection Accepted");
			}
			ObjectOutputStream outputStream = new ObjectOutputStream(clientSocket.getOutputStream());
			ObjectInputStream inputStream = new ObjectInputStream(clientSocket.getInputStream());

			Commande cmd = (Commande) inputStream.readObject();
			if (Settings.DEBUG)
				System.out.println(cmd.getCmd() + " " + cmd.getformat() + " " + cmd.getNomChunk() + " "
						+ InetAddress.getLocalHost().getHostName());

			switch (cmd.getCmd()) {
			case Commande_Read:
				readHDFS(cmd, outputStream);
				outputStream.close();
				break;
			case Commande_WRITE:
				writeHDFS(cmd, inputStream);
				break;
			case Commande_Delete:
				deleteHDFS(cmd.getNomChunk());
				break;
			}

			outputStream.close();
			inputStream.close();
			clientSocket.close();

		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	public static void main(String[] args) throws IOException {
		String ip = args[0];
		ServerSocket server = null;

		try {

			// register datanode
			DataNode dataNodeInfo = new DataNode(ip, Settings.HDFS_PORT);
			// System.setProperty("java.rmi.Server.hostname", nameNodeIp);
			Registry registry = LocateRegistry.getRegistry(Settings.HDFS_HOST, Settings.HDFS_PORT);
			if (Settings.DEBUG) {
				System.out.println(Settings.HDFS_HOST + ":" + Settings.HDFS_PORT);
				System.out.println(ip + ":" + Settings.HDFS_PORT);
			}
			NameNode nameNode = (NameNode) registry.lookup(Settings.HDFS_NAME);
			// NameNode nameNode = (NameNode) Naming.lookup("//" + namenodeName + ":" +
			// nameNodePort + "/nameNodeDaemon");

			nameNode.addDataNode(dataNodeInfo);
			// nameNode.addDataNode(dataNodeInfo2);
			// nameNode.addDataNode(dataNodeInfo3);

			server = new ServerSocket(Settings.HDFS_PORT);
			while (true) {
				if (Settings.DEBUG)
					System.out.println("attente " + server.toString());
				Socket s = server.accept();
				if (Settings.DEBUG)
					System.out.println("accepted");

				HdfsServer hdfsserver = new HdfsServer();
				hdfsserver.setClientSocket(s);
				hdfsserver.start();
			}

		} catch (IOException | NotBoundException e) {
			e.printStackTrace();
		} finally {
			server.close();
		}
	}

}
