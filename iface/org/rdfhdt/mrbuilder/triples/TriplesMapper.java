/**
 * Author: Jose M. Gimenez-Garcia: josemiguel.gimenez@alumnos.uva.es
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * Contacting the authors:
 *   Jose M. Gimenez-Garcia: 	josemiguel.gimenez@alumnos.uva.es
 *   Javier D. Fernandez:       jfergar@infor.uva.es, javier.fernandez@wu.ac.at
 *   Miguel A. Martinez-Prieto: migumar2@infor.uva.es
 */
package org.rdfhdt.mrbuilder.triples;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.rdfhdt.hdt.dictionary.impl.FourSectionDictionary;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.io.CountInputStream;
import org.rdfhdt.mrbuilder.HDTBuilderConfiguration;
import org.rdfhdt.mrbuilder.HDTBuilderDriver.Counters;
import org.rdfhdt.mrbuilder.io.TripleWritable;

@SuppressWarnings("rawtypes")
public abstract class TriplesMapper<K extends TripleWritable, V extends WritableComparable> extends Mapper<LongWritable, Text, K, V> implements ProgressListener {

    protected FourSectionDictionary   dictionary;
    protected HDTBuilderConfiguration conf;

    /*
     * (non-Javadoc)
     *
     * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    Path[] cache = DistributedCache.getLocalCacheFiles(context.getConfiguration());
    	
	this.conf = new HDTBuilderConfiguration(context.getConfiguration());
	CountInputStream input = new CountInputStream(new BufferedInputStream(new FileInputStream(cache[0].toString())));
	File file = new File(cache[0].toString());
	this.dictionary = new FourSectionDictionary(this.conf.getSpec());
	this.dictionary.mapFromFile(input, file, this);
	input.close();

	// DEBUG
	// ((PFCDictionarySection) this.dictionary.getShared()).dumpAll();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	TripleString tripleString = new TripleString();

	try {
	    tripleString.read(value.toString());
	} catch (ParserException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	context.write(this.key(tripleString), this.value(tripleString));
	context.getCounter(Counters.Triples).increment(1);
    }

    @Override
    public void notifyProgress(float level, String message) {
	// if (!this.conf.getQuiet()) {
	System.out.print("\r" + message + "\t" + Float.toString(level) + "                            \r");
    }

    protected abstract K key(TripleString tripleString) throws InterruptedException;

    protected abstract V value(TripleString tripleString);

}
