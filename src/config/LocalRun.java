package config;

import application.QuasiMonteCarlo;
import formats.Format;
import ordo.JobLocal;

public class LocalRun {

	public static void main(String args[]) {
		System.out.println("Lancement en local.");
		JobLocal j = new JobLocal();
		j.setInputFormat(Format.Type.LINE);
		j.setInputFname(args[0]);
		long t1 = System.currentTimeMillis();
		j.startJob(new QuasiMonteCarlo());
		long t2 = System.currentTimeMillis();
		System.out.println("time in ms =" + (t2 - t1));
		System.exit(0);
	}
}
