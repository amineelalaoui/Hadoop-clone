package application;

import formats.Format;
import ordo.JobLocal;

public class CharCountLocal extends CharCount {
	private static final long serialVersionUID = 1L;

	public static void main(String args[]) {
		JobLocal j = new JobLocal();
        j.setInputFormat(Format.Type.LINE);
        j.setInputFname(args[0]);
        long t1 = System.currentTimeMillis();
		j.startJob(new CharCount());
		long t2 = System.currentTimeMillis();
        System.out.println("time in ms ="+(t2-t1));
        System.exit(0);
		}
}
