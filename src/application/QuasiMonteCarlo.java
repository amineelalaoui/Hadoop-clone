package application;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import config.Settings;
import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;

public class QuasiMonteCarlo implements MapReduce {

	private static class HaltonSequence {
		/** Bases */
		static final int[] P = { 2, 3 };
		/** Maximum number of digits allowed */
		static final int[] K = { 63, 40 };

		private long index;
		private double[] x;
		private double[][] q;
		private int[][] d;

		/**
		 * Initialize to H(startindex), so the sequence begins with H(startindex+1).
		 */
		HaltonSequence(long startindex) {
			index = startindex;
			x = new double[K.length];
			q = new double[K.length][];
			d = new int[K.length][];
			for (int i = 0; i < K.length; i++) {
				q[i] = new double[K[i]];
				d[i] = new int[K[i]];
			}

			for (int i = 0; i < K.length; i++) {
				long k = index;
				x[i] = 0;

				for (int j = 0; j < K[i]; j++) {
					q[i][j] = (j == 0 ? 1.0 : q[i][j - 1]) / P[i];
					d[i][j] = (int) (k % P[i]);
					k = (k - d[i][j]) / P[i];
					x[i] += d[i][j] * q[i][j];
				}
			}
		}

		/**
		 * Compute next point. Assume the current point is H(index). Compute H(index+1).
		 * 
		 * @return a 2-dimensional point with coordinates in [0,1)^2
		 */
		double[] nextPoint() {
			index++;
			for (int i = 0; i < K.length; i++) {
				for (int j = 0; j < K[i]; j++) {
					d[i][j]++;
					x[i] += q[i][j];
					if (d[i][j] < P[i]) {
						break;
					}
					d[i][j] = 0;
					x[i] -= (j == 0 ? 1.0 : q[i][j - 1]);
				}
			}
			return x;
		}
	}

	@Override
	public void map(FormatReader reader, FormatWriter writer) {
		Map<String, Long> hm = new HashMap<>();
		KV kv;
		while ((kv = reader.read()) != null) {
			StringTokenizer st = new StringTokenizer(kv.v);
			while (st.hasMoreTokens()) {
				String tok = st.nextToken();
				String[] params = tok.split(";");
				String start = params[0];
				String end = params[1];
				long start_i = Long.parseLong(start);
				long end_i = Long.parseLong(end);
				final HaltonSequence haltonsequence = new HaltonSequence(start_i);
				long numInside = 0L;
				long numOutside = 0L;
				if (Settings.DEBUG)
					System.out.println("calcul MC from " + start_i + " to " + (end_i));
				for (long i = 0; i < end_i - start_i; i++) {
					// generate points in a unit square
					final double[] point = haltonsequence.nextPoint();
					// count points inside/outside of the inscribed circle of the square
					final double x = point[0] - 0.5;
					final double y = point[1] - 0.5;
					if (x * x + y * y > 0.25) {
						numOutside++;
					} else {
						numInside++;
					}
				}
				if (hm.containsKey("in"))
					hm.put("in", hm.get("in").longValue() + numInside);
				else
					hm.put("in", numInside);

				if (hm.containsKey("out"))
					hm.put("out", hm.get("out").longValue() + numOutside);
				else
					hm.put("out", numOutside);
				if (Settings.DEBUG)
					System.out.println("end MC from " + start_i + " to " + (end_i - start_i));
			}
		}
		for (String k : hm.keySet())
			writer.write(new KV(k, hm.get(k).toString()));
	}

	@Override
	public void reduce(FormatReader reader, FormatWriter writer) {
		Map<String, Long> hm = new HashMap<>();
		KV kv;
		while ((kv = reader.read()) != null) {
			if (hm.containsKey(kv.k))
				hm.put(kv.k, hm.get(kv.k) + Long.parseLong(kv.v));
			else
				hm.put(kv.k, Long.parseLong(kv.v));
		}
		float in = hm.get("in");
		float out = hm.get("out");
		double approx = 4 * in / (out + in);
		System.out.println("in:" + in + " out:" + out + " => pi:" + approx);
		writer.write(new KV("pi", Double.toString(approx)));
	}

	public static void main(String[] args) {
		Job j = new Job();
		j.setInputFormat(Format.Type.LINE);
		j.setInputFname(args[0]);
		long t1 = System.currentTimeMillis();
		j.startJob(new QuasiMonteCarlo());
		long t2 = System.currentTimeMillis();
		System.out.println("time in ms =" + (t2 - t1));
		System.exit(0);
	}

}
