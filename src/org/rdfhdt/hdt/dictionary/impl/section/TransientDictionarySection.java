package org.rdfhdt.hdt.dictionary.impl.section;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.rdfhdt.hdt.compact.integer.VByte;
import org.rdfhdt.hdt.compact.sequence.SequenceLog64;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.trans.TransientElement;
import org.rdfhdt.hdt.util.Mutable;
import org.rdfhdt.hdt.util.crc.CRC32;
import org.rdfhdt.hdt.util.crc.CRC8;
import org.rdfhdt.hdt.util.crc.CRCOutputStream;
import org.rdfhdt.hdt.util.io.IOUtil;
import org.rdfhdt.hdt.util.string.ByteStringUtil;
import org.rdfhdt.hdt.util.string.CompactString;
import org.rdfhdt.hdt.util.string.ReplazableString;

public class TransientDictionarySection extends PFCDictionarySectionBig implements TransientElement {

	ByteArrayOutputStream	byteOut;
	CharSequence			previousStr;
	int						buffer;
	int						blockPerBuffer;
	long					storedBuffersSize;

	public TransientDictionarySection(HDTOptions spec) {
		super(spec);
		this.blocksize = (int) spec.getInt("pfc.blocksize");
		if (this.blocksize == 0) {
			this.blocksize = DEFAULT_BLOCK_SIZE;
		}
		if (this.blockPerBuffer == 0) {
			this.blockPerBuffer = BLOCK_PER_BUFFER;
		}
	}

	@Override
	public void initialize(long numentries) {
		this.blocks = new SequenceLog64(63, numentries / this.blocksize);
		this.storedBuffersSize = 0;
		this.numstrings = 0;
		this.byteOut = new ByteArrayOutputStream(16 * 1024);
		this.blockPerBuffer = BLOCK_PER_BUFFER / 5;
		this.data = new byte[(int) Math.ceil((((double) numentries / this.blocksize) / this.blockPerBuffer))][];
		this.posFirst = new long[this.data.length];
		this.buffer = 0;
		this.previousStr = null;
	}

	@Override
	public void load(SequenceFile.Reader input, ProgressListener listener) throws IOException {
		CharSequence str = null;
		Text line = new Text();

		this.posFirst[0] = 0;
		while (input.next(line)) {
			str = new CompactString(line.toString());

			if (this.numstrings % this.blocksize == 0) {
				// Add new block pointer
				// System.out.println(this.storedBuffersSize);
				// System.out.println(this.byteOut.size());
				// System.out.println(this.blocksize);
				this.blocks.append(this.storedBuffersSize + this.byteOut.size());

				// if number of block per buffer reached, change buffer
				if (((this.blocks.getNumberOfElements() - 1) % this.blockPerBuffer == 0) && ((this.blocks.getNumberOfElements() - 1) / this.blockPerBuffer != 0)) {
					this.storedBuffersSize += this.byteOut.size();
					this.storeBuffer(this.buffer);
					this.byteOut = new ByteArrayOutputStream(16 * 1024);
					if (this.buffer < this.data.length - 1) {
						this.posFirst[++this.buffer] = this.storedBuffersSize + this.byteOut.size();
					}
				}

				// Copy full string
				ByteStringUtil.append(this.byteOut, str, 0);
			} else {
				// Find common part.
				int delta = ByteStringUtil.longestCommonPrefix(this.previousStr, str);
				// Write Delta in VByte
				VByte.encode(this.byteOut, delta);
				// Write remaining
				ByteStringUtil.append(this.byteOut, str, delta);
			}

			// System.out.println(str);

			this.byteOut.write(0); // End of string
			this.numstrings++;
			this.previousStr = str;
		}
	}

	protected void storeBuffer(int buffer) throws IOException {
		// System.out.println("Buffer = " + buffer);
		this.byteOut.flush();
		this.data[buffer] = this.byteOut.toByteArray();
		this.byteOut.close();
	}

	@Override
	public void close() throws IOException {
		// Ending block pointer.
		this.blocks.append(this.storedBuffersSize + this.byteOut.size());

		// Trim text/blocks
		this.blocks.aggresiveTrimToSize();

		// System.out.println("Data length = " + this.data.length);
		this.storeBuffer(this.buffer);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.rdfhdt.hdt.dictionary.impl.section.PFCDictionarySectionBig#save(java.io.OutputStream, org.rdfhdt.hdt.listener.ProgressListener)
	 */
	@Override
	public void save(OutputStream output, ProgressListener listener) throws IOException {
		long dataLenght = 0;
		CRCOutputStream out = new CRCOutputStream(output, new CRC8());

		for (byte[] buffer : this.data) {
			dataLenght += buffer.length;
		}

		out.write(TYPE_INDEX);
		VByte.encode(out, this.numstrings);
		VByte.encode(out, dataLenght);
		VByte.encode(out, this.blocksize);

		out.writeCRC();

		this.blocks.save(output, listener); // Write blocks directly to output, they have their own CRC check.

		out.setCRC(new CRC32());
		for (byte[] buffer : this.data) {
			IOUtil.writeBuffer(out, buffer, 0, buffer.length, listener);
		}
		out.writeCRC();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see hdt.dictionary.DictionarySection#extract(int)
	 */
	@Override
	public CharSequence extract(int id) {

		// System.out.println("id = " + id);

		if (id < 1 || id > this.numstrings) {
			return null;
		}

		// Locate block
		int blockid = (id - 1) / this.blocksize;
		int nstring = (id - 1) % this.blocksize;

		// System.out.println("blockid = " + blockid);
		// System.out.println("nstring = " + nstring);

		byte[] block = this.data[blockid / this.blockPerBuffer];
		int pos = (int) (this.blocks.get(blockid) - this.posFirst[blockid / this.blockPerBuffer]);

		// System.out.println("pos = " + pos);

		// Copy first string
		int len = ByteStringUtil.strlen(block, pos);

		// System.out.println("len = " + len);

		Mutable<Long> delta = new Mutable<Long>(0L);
		ReplazableString tempString = new ReplazableString();
		tempString.append(block, pos, len);

		// System.out.println("dentro del for");

		// Copy strings untill we find our's.
		for (int i = 0; i < nstring; i++) {
			pos += len + 1;
			// System.out.println("pos = " + pos);
			pos += VByte.decode(block, pos, delta);
			// System.out.println("pos = " + pos);
			// System.out.println("delta = [" + delta + "]");
			len = ByteStringUtil.strlen(block, pos);
			// System.out.println("len = " + len);
			tempString.replace(delta.getValue().intValue(), block, pos, len);
			// System.out.println("tempstring = [" + tempString + "]");
		}
		return tempString;
	}

	/**
	 * Locate the block of a string doing binary search.
	 */
	@Override
	protected int locateBlock(CharSequence str) {
		int low = 0;
		int high = (int) this.blocks.getNumberOfElements() - 1;
		int max = high;

		while (low <= high) {
			int mid = (low + high) >>> 1;

		int cmp;
		if (mid == max) {
			cmp = -1;
		} else {
			cmp = ByteStringUtil.strcmp(str, this.data[mid / this.blockPerBuffer], (int) (this.blocks.get(mid) - this.posFirst[mid / this.blockPerBuffer]));

			// if (str.toString().contains("http://dbpedia.org/ontology/Agent") || str.toString().contains("The Health Inspector pays a visit") || str.toString().contains("Crockett_Middle_School") || str.toString().contains("Benthosuchus")) {
			// System.out.println("Block: "+ mid + ": "+ ByteStringUtil.asString(data[mid / blockPerBuffer], (int) (this.blocks.get(mid) - this.posFirst[mid / blockPerBuffer])) + " Result: " + cmp);
			// }
		}

		if (cmp < 0) {
			high = mid - 1;
		} else if (cmp > 0) {
			low = mid + 1;
		} else {
			return mid; // key found
		}
		}
		return -(low + 1); // key not found.
	}

	@Override
	protected int locateInBlock(int blockid, CharSequence str) {

		ReplazableString tempString = new ReplazableString();

		Mutable<Long> delta = new Mutable<Long>(0L);
		int idInBlock = 0;
		int cshared = 0;

		byte[] block = this.data[blockid / this.blockPerBuffer];
		int pos = (int) (this.blocks.get(blockid) - this.posFirst[blockid / this.blockPerBuffer]);

		// Read the first string in the block
		int slen = ByteStringUtil.strlen(block, pos);
		tempString.append(block, pos, slen);
		pos += slen + 1;
		idInBlock++;

		while ((idInBlock < this.blocksize) && (pos < block.length)) {
			// Decode prefix
			pos += VByte.decode(block, pos, delta);

			// Copy suffix
			slen = ByteStringUtil.strlen(block, pos);
			tempString.replace(delta.getValue().intValue(), block, pos, slen);

			if (delta.getValue() >= cshared) {
				// Current delta value means that this string
				// has a larger long common prefix than the previous one
				// if (str.toString().contains("http://dbpedia.org/ontology/Agent") || str.toString().contains("The Health Inspector pays a visit") || str.toString().contains("Crockett_Middle_School") || str.toString().contains("Benthosuchus")) {
				// System.out.println("[" + tempString + "]. cshared [" + cshared + "]");
				// }
				cshared += ByteStringUtil.longestCommonPrefix(tempString, str, cshared);

				if ((cshared == str.length()) && (tempString.length() == str.length())) {
					break;
				}
			} else {
				// We have less common characters than before,
				// this string is bigger that what we are looking for.
				// i.e. Not found.
				idInBlock = 0;
				break;
			}
			pos += slen + 1;
			idInBlock++;

		}

		// Not found
		if (pos == block.length || idInBlock == this.blocksize) {
			idInBlock = 0;
		}

		return idInBlock;
	}

}
