package config;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import hdfs.NameNode;
import map.MapReduce;

public class Project {

	private static Class<?> interfaceMapReduce = null;

	static public void initialiser(String nomImpl, String filename, boolean local) {
		Map<String, List<String>> mapping = new HashMap<>();
		try {
			Registry registry = LocateRegistry.getRegistry(Settings.HDFS_HOST, Settings.HDFS_PORT);
			NameNode nameNode = (NameNode) registry.lookup(Settings.HDFS_NAME);
			mapping = nameNode.getHostsByFiles();
		} catch (RemoteException | NotBoundException e1) {
			e1.printStackTrace();
		}

		int nbHosts = mapping.size();
		nomImpl = local ? nomImpl + "Local" : nomImpl;
		String strMaps = local ? "" : " en utilisant " + nbHosts + " hosts.";
		System.out.println("Lancement de " + nomImpl + " sur le fichier " + filename + strMaps);
		MapReduce mr = loadImpl("map.MapReduce", nomImpl);

		String name = Settings.HIDOOP_HOST;
		String cmd = "ssh " + name + " java --class-path " + Settings.BIN_PATH + " " + mr.getClass().getName() + " "
				+ Settings.DATA_PATH + filename;
		Process p = null;
		int end = -1;
		try {
			p = Runtime.getRuntime().exec(cmd);
			end = p.waitFor();
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String s = null;
			while ((s = in.readLine()) != null) {
				System.out.println(s);
			}
			if (end == 0) {

			} else {
				System.out.println("Echec de l'execution...");
			}
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		// new IHMJob (); -> Dans le map dans ce cas là
		System.exit(0);
	}

	public static Class<?> getInterfaceMapReduce() {
		if (interfaceMapReduce == null) {
			return calculInterfaceMapReduce("MapReduce");
		} else {
			return interfaceMapReduce;
		}
	}

	private static Class<?> calculInterfaceMapReduce(String interfaceName) {
		Class<?> interf = null;
		String[] packages = (new File(Settings.BIN_PATH)).list();
		// Recherche des classes implémentant l'interface trouvé
		for (String pack : packages) {
			try {
				interf = Class.forName(pack + "." + interfaceName);
			} catch (ClassNotFoundException e) {
				// System.err.println ("Panic: ne trouve pas l'interface " + interfaceName+"
				// dans : " +e);
			}
		}
		// Cas où on n'a pas trouvé l'interface dans tous les packages
		if (interf == null) {
			System.err.println("Impossible de trouver l'interface " + interfaceName);
			System.exit(1);
		}
		return interf;
	}

	private static MapReduce loadImpl(String interfName, String implName) {
		MapReduce res = null;
		// Obtenir l'interface interfName
		Class<?> interf = Project.getInterfaceMapReduce();

		// Trouve la classe implName (ou interfName_implName)
		String[] packages = (new File(Settings.BIN_PATH)).list();
		// Recherche des classes implémentant l'interface trouvé
		Class<?> implant = null;
		for (String pack : packages) {
			try {
				implant = Class.forName(pack + "." + implName);
			} catch (ClassNotFoundException e1) {
				try {
					implant = Class.forName(interfName + "_" + implName);
				} catch (ClassNotFoundException e2) {
					// System.err.println ("Impossible de trouver la classe "+implName+": "+e1);
				}
			}
		}

		// Vérifie qu'elle implante la bonne interface
		if (!interf.isAssignableFrom(implant)) {
			System.err.println(
					"La classe " + implant.getName() + " n'implante pas l'interface " + interf.getName() + ".");
			return null;
		}

		// Crée une instance de cette classe
		try {
			Constructor cons = implant.getConstructor();
			Object[] initargs = {};
			res = (MapReduce) cons.newInstance(initargs);
		} catch (NoSuchMethodException e) {
			System.err.println("Classe " + implant.getName() + ": pas de constructeur adequat: " + e);
		} catch (InstantiationException e) {
			System.err.println("Echec instation " + implant.getName() + ": " + e);
		} catch (IllegalAccessException e) {
			System.err.println("Echec instation " + implant.getName() + ": " + e);
		} catch (java.lang.reflect.InvocationTargetException e) {
			System.err.println("Echec instation " + implant.getName() + ": " + e);
			if (e.getCause() != null) {
				System.err.println(" La cause est : " + e.getCause() + " in " + (e.getCause().getStackTrace())[0]);
			}
		} catch (ClassCastException e) {
			System.err.println("Echec instation " + implant.getName() + ": n'est pas un " + interfName + ": " + e);
		}
		return res;
	}

	/*
	 * Paramètre de test Hidoop sans HDFS Les fichiers test_f1.txt (resp.
	 * test_f2.txt et test_f3.txt) sont sur les disques locales de nymphe (resp.
	 * grove et eomer)
	 */
	public static boolean HIDOOP_TEST = true;

	private static String[] names = { "nymphe", "grove", "eomer" };
	private static String[] files = { "filesample_1.txt", "filesample_2.txt", "filesample_3.txt" };

	private static HashMap<String, List<String>> genDic() {
		HashMap<String, List<String>> dic = new HashMap<>();
		for (int i = 0; i < files.length; i++) {
			List<String> l = new ArrayList<>();
			l.add(Settings.TMP_PATH + files[i]);
			dic.put(names[i], l);
		}
		return dic;
	}

	private static List<String> computers = new ArrayList<String>();

	private static List<String> pickComputers(int nbMap) {
		if (HIDOOP_TEST) {
			return Arrays.asList(Project.names);
			// } else if (Project.computers.size() != nbMap) {
			// List<String> machines = Arrays.asList(Machines.names);
			// Collections.shuffle(machines);
			// Project.computers = machines.subList(0, nbMap);
		}
		return Arrays.asList(Project.names);
		// return computers;
	}

	// Changer le nom de la méthode dic_test pour match avec HDFS
	public static HashMap<String, List<String>> getSplit() {
		if (HIDOOP_TEST) {
			return genDic();
		} else {
			// Project.nbMap
			// Call to HDFS pour split fname si pas déjà fait
			return null;
		}
	}

	public static void main(String args[]) {

		boolean local = false;
		if (args.length >= 2) {
			String application = args[0];
			String filename = args[1];
			if (args.length == 3) {
				local = true;
			}
			initialiser(application, filename, local);
		} else {
			System.out.println("Projet <application> <filename> <?local>");
		}
	}
}
