package org.rdfhdt.hdt.compact.sequence;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.rdfhdt.hdt.compact.integer.VByte;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.util.BitUtil;
import org.rdfhdt.hdt.util.crc.CRC32;
import org.rdfhdt.hdt.util.crc.CRC8;
import org.rdfhdt.hdt.util.crc.CRCOutputStream;
import org.rdfhdt.hdt.util.io.IOUtil;

public class TransientSequenceLog64 extends SequenceLog64 {

	protected OutputStream	tempOutput;
	protected long			bufferSize, maxentries;
	protected long			capacity;
	private long			totalentries, totalwords;

	protected FileSystem	fileSystem;
	protected Path			file;
	protected String		fileName;

	public TransientSequenceLog64(int bufferSize) throws IOException {
		this(bufferSize, W);
	}

	public TransientSequenceLog64(int bufferSize, int numbits) throws IOException {
		this(bufferSize, numbits, 0);
	}

	public TransientSequenceLog64(int bufferSize, int numbits, long capacity, boolean initialize) throws IOException {
		this(bufferSize, numbits, capacity);
		if (initialize) {
			this.numentries = capacity;
		}
	}

	public TransientSequenceLog64(int bufferSize, int numbits, long capacity) throws IOException {
		this(bufferSize, numbits, capacity, null, null);
	}

	public TransientSequenceLog64(int bufferSize, int numbits, long capacity, FileSystem fs, Path path) throws IOException {
		super(numbits, Math.min(bufferSize, capacity));

		this.capacity = capacity;

		// parameter provided as bytes, transform to entries
		this.maxentries = (int) ((W / (double) numbits) * bufferSize);

		this.fileName = UUID.randomUUID().toString();

		if (fs == null) {
			fs = FileSystem.getLocal(new Configuration());
		}
		if (path == null) {
			path = new Path(".");
		}

		this.fileSystem = fs;
		this.file = new Path(path, this.fileName);
		this.tempOutput = this.fileSystem.create(this.file);
	}

	@Override
	public long getNumberOfElements() {
		return this.totalentries;
	}

	@Override
	public void append(long value) {
		super.append(value);

		if (this.numentries >= this.maxentries && (lastWordNumBits(this.numbits, this.numentries) == 64)) {
			try {
				this.flushData();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	protected void flushData() throws IOException {
		// System.out.println("Flushing Sequence");

		this.totalentries += this.numentries;

		int numwords = (int) numWordsFor(this.numbits, this.numentries);

		this.totalwords += numwords;

		// System.out.println("Remaining bits =" + lastWordNumBits(this.numbits, this.numentries));

		for (int i = 0; i < numwords; i++) {
			IOUtil.writeLong(this.tempOutput, this.data[i]);
		}

		long size = numWordsFor(this.numbits, this.numentries);
		assert size >= 0 && size <= Integer.MAX_VALUE;

		this.data = new long[Math.max((int) size, 1)];
		this.numentries = 0;
	}

	public void close() throws IOException {

		this.totalentries += this.numentries;

		int numwords = (int) numWordsFor(this.numbits, this.numentries);

		this.totalwords += numwords;

		// System.out.println("Closing sequence.");
		// System.out.println("Writing " + this.totalentries + " entries");
		// System.out.println("There should be " + this.capacity + " entries");
		// System.out.println("Writing " + this.totalwords + "words");

		// System.out.println("Remaining bits =" + lastWordNumBits(this.numbits, this.numentries));

		for (int i = 0; i < numwords - 1; i++) {
			IOUtil.writeLong(this.tempOutput, this.data[i]);
		}

		if (numwords > 0) {
			// Write only used bits from last entry (byte aligned, little endian)
			int lastWordUsedBits = lastWordNumBits(this.numbits, this.numentries);
			BitUtil.writeLowerBitsByteAligned(this.data[numwords - 1], lastWordUsedBits, this.tempOutput);
		}

		this.tempOutput.flush();
		this.tempOutput.close();

		this.data = new long[0];
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		CRCOutputStream out = new CRCOutputStream(output, new CRC8());

		out.write(SequenceFactory.TYPE_SEQLOG);
		out.write(this.numbits);
		VByte.encode(out, this.totalentries);
		out.writeCRC();
		out.setCRC(new CRC32());

		// long bytesCopied = Files.copy(this.fileSystem.open(this.file), out);
		long bytesCopied = IOUtils.copy(this.fileSystem.open(this.file), out);
		System.out.println("bytes copied from " + this.fileName + " = " + bytesCopied);
		this.fileSystem.delete(this.file, true);

		// System.out.println("CRC = " + out.getCRC().getValue());
		out.writeCRC();
	}
}
