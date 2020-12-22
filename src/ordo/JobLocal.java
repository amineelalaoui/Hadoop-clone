package ordo;

import formats.Format;
import formats.Format.OpenMode;
import formats.Format.Type;
import formats.KVFormat;
import formats.LineFormat;
import map.MapReduce;

public class JobLocal implements JobInterface {

	private Type inputFormat;
	private String fname;

	@Override
	public void setInputFormat(Type ft) {
		this.inputFormat = ft;
	}

	@Override
	public void setInputFname(String fname) {
		this.fname = fname;
	}

	@Override
	public void startJob(MapReduce mr) {
		Format reader = null;
		if (this.inputFormat == Type.LINE) {
			reader = new LineFormat(this.fname);
		} else {
			reader = new KVFormat(this.fname);
		}
		reader.open(OpenMode.R);
		int index_dot = this.fname.indexOf('.');
		String _fname = this.fname.substring(0, index_dot);
		String ext = this.fname.substring(index_dot);
		Format writer = new KVFormat(_fname + "-res" + ext);
		writer.open(OpenMode.W);
		mr.map(reader, writer);
		System.out.println("Le résultat se trouve dans " + _fname + "-res" + ext);
	}

}
