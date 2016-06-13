package org.rdfhdt.hdt.triples.impl;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.rdfhdt.hdt.compact.bitmap.AdjacencyList;
import org.rdfhdt.hdt.compact.bitmap.TransientBitmap375;
import org.rdfhdt.hdt.compact.sequence.TransientSequenceLog64;
import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.exceptions.IllegalFormatException;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.util.BitUtil;
import org.rdfhdt.hdt.util.listener.ListenerUtil;
import org.rdfhdt.mrbuilder.HDTBuilderConfiguration;
import org.rdfhdt.mrbuilder.io.TripleSPOWritable;

public class TransientBitMapTriples extends BitmapTriples {

	long		number;
	long		size;
	long		lastX		= 0, lastY = 0, lastZ = 0;
	long		x, y, z;
	long		numTriples	= 0;
	boolean		trimNeeded	= false;

	FileSystem	fileSystem;
	Path		path;

	public TransientBitMapTriples() {
		super();
	}

	public TransientBitMapTriples(HDTOptions spec) {
		super(spec);
	}

	public TransientBitMapTriples(FileSystem fs, Path path) {
		this();
		this.setFileSystem(fs);
		this.setPath(path);
	}

	public TransientBitMapTriples(HDTOptions spec, FileSystem fs, Path path) {
		this(spec);
		this.setFileSystem(fs);
		this.setPath(path);
	}

	public void setFileSystem(FileSystem fs) {
		this.fileSystem = fs;
	}

	public void setPath(Path path) {
		this.path = path;
	}

	public void initialize(long numentries) throws IOException {
		this.initialize(numentries, numentries, numentries);
		this.trimNeeded = true;
	}

	public void initialize(long numentries, long maxvalue) throws IOException {
		this.initialize(numentries, maxvalue, maxvalue);
		this.trimNeeded = true;
	}

	public void initialize(long numentries, long maxpredicate, long maxobject) throws IOException {

		// System.out.println("Numentries: " + numentries);

		this.number = numentries;
		this.seqY = new TransientSequenceLog64(HDTBuilderConfiguration.CHUNK_SIZE, BitUtil.log2(maxpredicate), this.number, this.fileSystem, this.path);
		this.seqZ = new TransientSequenceLog64(HDTBuilderConfiguration.CHUNK_SIZE, BitUtil.log2(maxobject), this.number, this.fileSystem, this.path);
		this.bitmapY = new TransientBitmap375(HDTBuilderConfiguration.CHUNK_SIZE, this.number, this.fileSystem, this.path);
		this.bitmapZ = new TransientBitmap375(HDTBuilderConfiguration.CHUNK_SIZE, this.number, this.fileSystem, this.path);
		// this.bitmapY = new Bitmap375(this.number);
		// this.bitmapZ = new Bitmap375(this.number);
	}

	public void load(SequenceFile.Reader input, ProgressListener listener) throws IOException {
		TripleSPOWritable tripleWritable = new TripleSPOWritable();

		while (input.next(tripleWritable)) {
			TripleID triple = new TripleID((int) tripleWritable.getSubject().get(), (int) tripleWritable.getPredicate().get(), (int) tripleWritable.getObject().get());
			this.add(triple);
			ListenerUtil.notifyCond(listener, "Converting to BitmapTriples", this.numTriples, this.numTriples, this.number);
			this.numTriples++;
		}
	}

	public void add(TripleID triple) {
		TransientSequenceLog64 vectorY = (TransientSequenceLog64) this.seqY;
		TransientSequenceLog64 vectorZ = (TransientSequenceLog64) this.seqZ;
		TransientBitmap375 bitY = (TransientBitmap375) this.bitmapY;
		TransientBitmap375 bitZ = (TransientBitmap375) this.bitmapZ;
		// Bitmap375 bitY = (Bitmap375) this.bitmapY;
		// Bitmap375 bitZ = (Bitmap375) this.bitmapZ;

		TripleOrderConvert.swapComponentOrder(triple, TripleComponentOrder.SPO, this.order);
		this.x = triple.getSubject();
		this.y = triple.getPredicate();
		this.z = triple.getObject();

		if (this.x == 0 || this.y == 0 || this.z == 0) {
			throw new IllegalFormatException("None of the components of a triple can be null");
		}

		if (this.numTriples == 0) {
			// First triple
			vectorY.append(this.y);
			vectorZ.append(this.z);
		} else if (this.x != this.lastX) {
			if (this.x != this.lastX + 1) {
				throw new IllegalFormatException("Upper level must be increasing and correlative.");
			}
			// X changed
			bitY.append(true);
			vectorY.append(this.y);

			bitZ.append(true);
			vectorZ.append(this.z);
		} else if (this.y != this.lastY) {
			if (this.y < this.lastY) {
				throw new IllegalFormatException("Middle level must be increasing for each parent.");
			}

			// Y changed
			bitY.append(false);
			vectorY.append(this.y);

			bitZ.append(true);
			vectorZ.append(this.z);
		} else if (this.z != this.lastZ) { // Añadido para quitar triples duplicados
			if (this.z < this.lastZ) {
				throw new IllegalFormatException("Lower level must be increasing for each parent.");
			}

			// Z changed
			bitZ.append(false);
			vectorZ.append(this.z);
		}

		this.lastX = this.x;
		this.lastY = this.y;
		this.lastZ = this.z;
	}

	public void close() throws IOException {
		TransientSequenceLog64 vectorY = (TransientSequenceLog64) this.seqY;
		TransientSequenceLog64 vectorZ = (TransientSequenceLog64) this.seqZ;
		TransientBitmap375 bitY = (TransientBitmap375) this.bitmapY;
		TransientBitmap375 bitZ = (TransientBitmap375) this.bitmapZ;
		// Bitmap375 bitY = (Bitmap375) this.bitmapY;
		// Bitmap375 bitZ = (Bitmap375) this.bitmapZ;

		bitY.append(true);
		bitZ.append(true);

		bitY.close();
		bitZ.close();

		vectorY.close();
		vectorZ.close();

		// System.out.println("bitmapY size = " + this.bitmapY.getNumBits());
		// System.out.println("seqY size = " + this.seqY.getNumberOfElements());
		// System.out.println("bitmapZ size = " + this.bitmapZ.getNumBits());
		// System.out.println("seqZ size = " + this.seqZ.getNumberOfElements());

		if (this.trimNeeded) {
			vectorY.aggresiveTrimToSize();
			vectorZ.trimToSize();
		}

		this.adjY = new AdjacencyList(this.seqY, this.bitmapY);
		this.adjZ = new AdjacencyList(this.seqZ, this.bitmapZ);

		// DEBUG
		// this.adjY.dump();
		// this.adjZ.dump();
	}

	@Override
	public long getNumberOfElements() {
		return this.number;
	}

	@Override
	public long size() {
		return this.size;
	}

}
