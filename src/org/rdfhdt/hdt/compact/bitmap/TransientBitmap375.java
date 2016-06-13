package org.rdfhdt.hdt.compact.bitmap;

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

public class TransientBitmap375 extends Bitmap375 {

	protected OutputStream	tempOutput;
	protected int			bufferSize;
	protected int			previousWordIndex;
	protected long			nbits;
	private long			totalbits	= 0;
	private long			totalwords	= 0;

	protected FileSystem	fileSystem;
	protected Path			file;
	protected String		fileName;

	public TransientBitmap375(int bufferSize) {
		super();
		this.bufferSize = bufferSize;
		this.previousWordIndex = wordIndex(0);
	}

	public TransientBitmap375(int bufferSize, long nbits, FileSystem fs, Path path) throws IOException {
		super(Math.min(bufferSize, nbits));

		this.bufferSize = bufferSize;
		this.nbits = nbits;
		this.previousWordIndex = wordIndex(0);

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
	public long getNumBits() {
		return this.totalbits;
	}

	// @Override
	// public void append(boolean value) {
	// this.set(this.numbits++, value);
	// }

	@Override
	public void set(long bitIndex, boolean value) {
		if ((this.previousWordIndex >= this.bufferSize) && (this.previousWordIndex != wordIndex(bitIndex))) {
			try {
				// System.out.println("bitIndex = " + bitIndex);
				// System.out.println("numbits = " + this.numbits);
				this.flushData();
				super.set(0, value);
				this.previousWordIndex = wordIndex(0);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			super.set(bitIndex, value);
			this.previousWordIndex = wordIndex(bitIndex);
		}
	}

	private void flushData() throws IOException {

		// System.out.println("flushing bitmap " + this.fileName + " with " + this.numbits + " bits");
		// System.out.println("Bits from last word = " + lastWordNumBits(this.numbits));

		this.totalbits += this.numbits - 1;

		int numwords = (int) numWords(this.numbits - 1);

		this.totalwords += numwords;

		for (int i = 0; i < numwords; i++) {
			IOUtil.writeLong(this.tempOutput, this.words[i]);
		}
		this.words = new long[(int) numWords(this.nbits)];
		this.numbits = 0;
		this.previousWordIndex = wordIndex(0);
	}

	public void close() throws IOException {

		this.totalbits += this.numbits;

		int numwords = (int) numWords(this.numbits);

		this.totalwords += numwords;

		// System.out.println("Closing bitmap.");
		// System.out.println("Writing " + this.totalbits + " bits");
		// System.out.println("There should be " + this.nbits + " bits");
		// System.out.println("Writing " + this.totalwords + "words");
		// System.out.println("Bits from last word = " + lastWordNumBits(this.numbits));

		for (int i = 0; i < numwords - 1; i++) {
			IOUtil.writeLong(this.tempOutput, this.words[i]);
		}

		if (numwords > 0) {
			// Write only used bits from last entry (byte aligned, little endian)
			int lastWordUsed = lastWordNumBits(this.numbits);
			BitUtil.writeLowerBitsByteAligned(this.words[numwords - 1], lastWordUsed, this.tempOutput);
		}

		this.tempOutput.flush();
		this.tempOutput.close();

		this.words = new long[0];
	}

	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		CRCOutputStream out = new CRCOutputStream(output, new CRC8());

		// Write Type and Numbits
		out.write(BitmapFactory.TYPE_BITMAP_PLAIN);
		VByte.encode(out, this.totalbits);

		// Write CRC
		out.writeCRC();

		// Setup new CRC
		out.setCRC(new CRC32());

		// FileInputStream input = new FileInputStream(this.fileName);
		// long bytesCopied = Files.copy(this.fileSystem.open(this.file), out);
		long bytesCopied = IOUtils.copyLarge(this.fileSystem.open(this.file), out);
		// input.close();
		this.fileSystem.delete(this.file, true);
		System.out.println("bytes copied from " + this.fileName + " = " + bytesCopied);

		// System.out.println("CRC = " + out.getCRC().getValue());
		out.writeCRC();

	}
}
