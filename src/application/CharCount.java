package application;

import java.util.HashMap;
import java.util.Map;

import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;

public class CharCount implements MapReduce {
	

	private static final long serialVersionUID = 1L;

	@Override
	public void map(FormatReader reader, FormatWriter writer) {
		
		Map<Character,Integer> hm = new HashMap<>();
		KV kv;
		while ((kv = reader.read()) != null) {
			for (char c : kv.v.toCharArray()) {
				if (hm.containsKey(c)) hm.put(c, hm.get(c).intValue()+1);
				else hm.put(c, 1);
			}
		}
		for (char c : hm.keySet()) writer.write(new KV(Character.toString(c) ,hm.get(c).toString()));
	}
	
	@Override
	public void reduce(FormatReader reader, FormatWriter writer) {
        Map<String,Integer> hm = new HashMap<>();
		KV kv;
		while ((kv = reader.read()) != null) {
			if (hm.containsKey(kv.k)) hm.put(kv.k, hm.get(kv.k)+Integer.parseInt(kv.v));
			else hm.put(kv.k, Integer.parseInt(kv.v));
		}
		for (String k : hm.keySet()) writer.write(new KV(k,hm.get(k).toString()));
	}

	public static void main(String args[]) {
		Job j = new Job();
        j.setInputFormat(Format.Type.LINE);
        j.setInputFname(args[0]);
        long t1 = System.currentTimeMillis();
		j.startJob(new CharCount());
		long t2 = System.currentTimeMillis();
        System.out.println("time in ms ="+(t2-t1));
        System.exit(0);
	}
}
