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
package org.rdfhdt.mrbuilder;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.rdfhdt.hdt.dictionary.impl.FourSectionDictionary;
import org.rdfhdt.hdt.dictionary.impl.FourSectionDictionary2;
import org.rdfhdt.hdt.dictionary.impl.section.TransientDictionarySection;
import org.rdfhdt.hdt.hdt.impl.TransientHDT;
import org.rdfhdt.hdt.options.ControlInformation;
import org.rdfhdt.hdt.trans.TransientElement;
import org.rdfhdt.hdt.triples.impl.TransientBitMapTriples;
import org.rdfhdt.listener.HDTBuilderListener;
import org.rdfhdt.mrbuilder.dictionary.DictionaryCombiner;
import org.rdfhdt.mrbuilder.dictionary.DictionaryMapper;
import org.rdfhdt.mrbuilder.dictionary.DictionaryReducer;
import org.rdfhdt.mrbuilder.dictionary.DictionarySamplerMapper;
import org.rdfhdt.mrbuilder.dictionary.DictionarySamplerReducer;
import org.rdfhdt.mrbuilder.io.TripleSPOComparator;
import org.rdfhdt.mrbuilder.io.TripleSPOWritable;
import org.rdfhdt.mrbuilder.triples.TriplesSPOMapper;
import org.rdfhdt.mrbuilder.util.FileStatusComparator;

import com.hadoop.mapreduce.LzoTextInputFormat;

public class HDTBuilderDriver {

	public enum Counters {
		Triples, Subjects, Predicates, Objects, Shared, Sample
	}

	protected HDTBuilderConfiguration	conf;
	protected HDTBuilderListener		listener;
	protected FileSystem				inputFS, dictionaryFS, triplesFS;
	protected Long						numTriples	= null, numShared = null, numSubjects = null, numPredicates = null, numObjects = null;
	protected FourSectionDictionary2	dictionary	= null;

	public HDTBuilderDriver(String[] args) throws IOException {

		// load configuration
		this.conf = new HDTBuilderConfiguration(args);

		this.listener = new HDTBuilderListener(this.conf);

		// get the FileSystem instances for each path
		this.inputFS = this.conf.getInputPath().getFileSystem(this.conf.getConfigurationObject());
		this.dictionaryFS = this.conf.getDictionaryOutputPath().getFileSystem(this.conf.getConfigurationObject());
		this.triplesFS = this.conf.getTriplesOutputPath().getFileSystem(this.conf.getConfigurationObject());

	}

	public static void main(String[] args) throws Exception {
		boolean ok = true;
		HDTBuilderDriver driver = new HDTBuilderDriver(args);

		if (ok && driver.conf.runDictionarySampling()) {
			if (driver.conf.getDictionaryReducers() == 1) {
				System.out.println("WARNING: Only one Reducer. Dictionary creation as a single job is more efficient.");
			}
			ok = driver.runDictionaryJobSampling();
		}

		if (ok && driver.conf.runDictionary()) {
			if (driver.conf.getDictionaryReducers() > 1) {
				ok = driver.runDictionaryJob();
			} else {
				ok = driver.runDictionaryJobWithOneJob();
			}
		}

		if (ok && driver.conf.buildDictionary()) {
			ok = driver.buildDictionary();
		}

		if (ok && driver.conf.runTriplesSampling()) {
			if (driver.conf.getTriplesReducers() == 1) {
				System.out.println("WARNING: Only one Reducer. Triples creation as a single job is more efficient.");
			}
			ok = driver.runTriplesJobSampling();
		}

		if (ok && driver.conf.runTriples()) {
			if (driver.conf.getTriplesReducers() > 1) {
				ok = driver.runTriplesJob();
			} else {
				ok = driver.runTriplesJobWithOneJob();
			}
		}

		if (ok && driver.conf.buildHDT()) {
			ok = driver.buidHDT();
		}

		System.exit(ok ? 0 : 1);
	}

	protected boolean runDictionaryJobSampling() throws IOException, ClassNotFoundException, InterruptedException {
		boolean jobOK;
		Job job = null;

		// if input path does not exists, fail
		if (!this.inputFS.exists(this.conf.getInputPath())) {
			System.out.println("Dictionary input path does not exist: " + this.conf.getInputPath());
			System.exit(-1);
		}

		// if samples path exists...
		if (this.dictionaryFS.exists(this.conf.getDictionarySamplesPath())) {
			if (this.conf.getDeleteDictionarySamplesPath()) { // ... and option provided, delete recursively
				this.dictionaryFS.delete(this.conf.getDictionarySamplesPath(), true);
			} else { // ... and option not provided, fail
				System.out.println("Dictionary samples path does exist: " + this.conf.getDictionarySamplesPath());
				System.out.println("Select other path or use option -ds to overwrite");
				System.exit(-1);
			}
		}

		// Job to create a SequenceInputFormat with Roles
		job = new Job(this.conf.getConfigurationObject(), this.conf.getDictionaryJobName() + " phase 1");
		job.setJarByClass(HDTBuilderDriver.class);

		System.out.println("input = " + this.conf.getInputPath());
		System.out.println("samples = " + this.conf.getDictionarySamplesPath());

		FileInputFormat.addInputPath(job, this.conf.getInputPath());
		FileOutputFormat.setOutputPath(job, this.conf.getDictionarySamplesPath());

		job.setInputFormatClass(LzoTextInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

		job.setMapperClass(DictionarySamplerMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setCombinerClass(DictionarySamplerReducer.class);
		job.setReducerClass(DictionarySamplerReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(this.conf.getDictionarySampleReducers());

		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, com.hadoop.compression.lzo.LzoCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

		jobOK = job.waitForCompletion(true);

		return jobOK;
	}

	protected boolean runDictionaryJob() throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
		boolean jobOK;
		Job job = null;
		BufferedWriter bufferedWriter;

		// if output path exists...
		if (this.dictionaryFS.exists(this.conf.getDictionaryOutputPath())) {
			if (this.conf.getDeleteDictionaryOutputPath()) { // ... and option provided, delete recursively
				this.dictionaryFS.delete(this.conf.getDictionaryOutputPath(), true);
			} else { // ... and option not provided, fail
				System.out.println("Dictionary output path does exist: " + this.conf.getDictionaryOutputPath());
				System.out.println("Select other path or use option -dd to overwrite");
				System.exit(-1);
			}
		}

		// Sample the SequenceInputFormat to do TotalSort and create final output
		job = new Job(this.conf.getConfigurationObject(), this.conf.getDictionaryJobName() + " phase 2");

		job.setJarByClass(HDTBuilderDriver.class);

		System.out.println("samples = " + this.conf.getDictionarySamplesPath());
		System.out.println("output = " + this.conf.getDictionaryOutputPath());

		FileInputFormat.addInputPath(job, this.conf.getDictionarySamplesPath());
		FileOutputFormat.setOutputPath(job, this.conf.getDictionaryOutputPath());

		job.setInputFormatClass(SequenceFileInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

		// Identity Mapper
		// job.setMapperClass(Mapper.class);
		job.setCombinerClass(DictionaryCombiner.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		job.setReducerClass(DictionaryReducer.class);

		job.setNumReduceTasks(this.conf.getDictionaryReducers());

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		System.out.println("Sampling started");
		InputSampler.writePartitionFile(job, new InputSampler.IntervalSampler<Text, Text>(this.conf.getSampleProbability()));
		String partitionFile = TotalOrderPartitioner.getPartitionFile(job.getConfiguration());
		URI partitionUri = new URI(partitionFile + "#" + TotalOrderPartitioner.DEFAULT_PATH);
		DistributedCache.addCacheFile(partitionUri, job.getConfiguration());
		DistributedCache.createSymlink(job.getConfiguration());
		System.out.println("Sampling finished");

		MultipleOutputs.addNamedOutput(job, HDTBuilderConfiguration.SHARED, SequenceFileOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, HDTBuilderConfiguration.SUBJECTS, SequenceFileOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, HDTBuilderConfiguration.PREDICATES, SequenceFileOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, HDTBuilderConfiguration.OBJECTS, SequenceFileOutputFormat.class, Text.class, NullWritable.class);

		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, com.hadoop.compression.lzo.LzoCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

		jobOK = job.waitForCompletion(true);

		this.numShared = job.getCounters().findCounter(Counters.Shared).getValue();
		this.numSubjects = job.getCounters().findCounter(Counters.Subjects).getValue();
		this.numPredicates = job.getCounters().findCounter(Counters.Predicates).getValue();
		this.numObjects = job.getCounters().findCounter(Counters.Objects).getValue();

		bufferedWriter = new BufferedWriter(new OutputStreamWriter(this.dictionaryFS.create(this.conf.getDictionaryCountersFile())));

		bufferedWriter.write(HDTBuilderConfiguration.SHARED + "=" + this.numShared + "\n");
		bufferedWriter.write(HDTBuilderConfiguration.SUBJECTS + "=" + this.numSubjects + "\n");
		bufferedWriter.write(HDTBuilderConfiguration.PREDICATES + "=" + this.numPredicates + "\n");
		bufferedWriter.write(HDTBuilderConfiguration.OBJECTS + "=" + this.numObjects + "\n");

		bufferedWriter.close();

		return jobOK;
	}

	protected boolean runDictionaryJobWithOneJob() throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
		boolean jobOK;
		Job job = null;
		BufferedWriter bufferedWriter;

		// if input path does not exists, fail
		if (!this.inputFS.exists(this.conf.getInputPath())) {
			System.out.println("Dictionary input path does not exist: " + this.conf.getInputPath());
			System.exit(-1);
		}

		// if output path exists...
		if (this.dictionaryFS.exists(this.conf.getDictionaryOutputPath())) {
			if (this.conf.getDeleteDictionaryOutputPath()) { // ... and option provided, delete recursively
				this.dictionaryFS.delete(this.conf.getDictionaryOutputPath(), true);
			} else { // ... and option not provided, fail
				System.out.println("Dictionary output path does exist: " + this.conf.getDictionaryOutputPath());
				System.out.println("Select other path or use option -dd to overwrite");
				System.exit(-1);
			}
		}

		// Launch job
		job = new Job(this.conf.getConfigurationObject(), this.conf.getTriplesJobName());
		job.setJarByClass(HDTBuilderDriver.class);

		FileInputFormat.addInputPath(job, this.conf.getInputPath());
		FileOutputFormat.setOutputPath(job, this.conf.getDictionaryOutputPath());

		job.setInputFormatClass(LzoTextInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

		job.setMapperClass(DictionaryMapper.class);
		job.setCombinerClass(DictionaryCombiner.class);
		job.setReducerClass(DictionaryReducer.class);

		job.setNumReduceTasks(this.conf.getDictionaryReducers());

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		MultipleOutputs.addNamedOutput(job, HDTBuilderConfiguration.SHARED, SequenceFileOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, HDTBuilderConfiguration.SUBJECTS, SequenceFileOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, HDTBuilderConfiguration.PREDICATES, SequenceFileOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, HDTBuilderConfiguration.OBJECTS, SequenceFileOutputFormat.class, Text.class, NullWritable.class);

		jobOK = job.waitForCompletion(true);

		this.numShared = job.getCounters().findCounter(Counters.Shared).getValue();
		this.numSubjects = job.getCounters().findCounter(Counters.Subjects).getValue();
		this.numPredicates = job.getCounters().findCounter(Counters.Predicates).getValue();
		this.numObjects = job.getCounters().findCounter(Counters.Objects).getValue();

		bufferedWriter = new BufferedWriter(new OutputStreamWriter(this.dictionaryFS.create(this.conf.getDictionaryCountersFile())));

		bufferedWriter.write(HDTBuilderConfiguration.SHARED + "=" + this.numShared + "\n");
		bufferedWriter.write(HDTBuilderConfiguration.SUBJECTS + "=" + this.numSubjects + "\n");
		bufferedWriter.write(HDTBuilderConfiguration.PREDICATES + "=" + this.numPredicates + "\n");
		bufferedWriter.write(HDTBuilderConfiguration.OBJECTS + "=" + this.numObjects + "\n");

		bufferedWriter.close();

		return jobOK;
	}

	protected boolean buildDictionary() throws IOException {
		FourSectionDictionary dictionary4mappers, dictionary4reducers;

		// if job not ran, read Counters
		if (!this.conf.runDictionary()) {

			System.out.println("Dictionary job not ran. Reading data from file.");

			BufferedReader reader = new BufferedReader(new InputStreamReader(this.dictionaryFS.open(this.conf.getDictionaryCountersFile())));
			String line = reader.readLine();
			while (line != null) {
				String[] data = line.split("=");
				switch (data[0]) {
					case HDTBuilderConfiguration.SHARED:
						this.numShared = Long.parseLong(data[1]);
						break;
					case HDTBuilderConfiguration.SUBJECTS:
						this.numSubjects = Long.parseLong(data[1]);
						break;
					case HDTBuilderConfiguration.PREDICATES:
						this.numPredicates = Long.parseLong(data[1]);
						break;
					case HDTBuilderConfiguration.OBJECTS:
						this.numObjects = Long.parseLong(data[1]);
				}
				line = reader.readLine();
			}
			reader.close();
		}

		TransientDictionarySection shared = new TransientDictionarySection(this.conf.getSpec());
		TransientDictionarySection subjects = new TransientDictionarySection(this.conf.getSpec());
		TransientDictionarySection predicates = new TransientDictionarySection(this.conf.getSpec());
		TransientDictionarySection objects = new TransientDictionarySection(this.conf.getSpec());

		System.out.println("Shared section = " + this.conf.getSharedSectionPath());

		this.loadFromDir(shared, this.numShared, this.dictionaryFS, this.conf.getSharedSectionPath());
		this.loadFromDir(subjects, this.numSubjects, this.dictionaryFS, this.conf.getSubjectsSectionPath());
		this.loadFromDir(predicates, this.numPredicates, this.dictionaryFS, this.conf.getPredicatesSectionPath());
		this.loadFromDir(objects, this.numObjects, this.dictionaryFS, this.conf.getObjectsSectionPath());

		System.out.println("Saving dictionary...");
		this.dictionary = new FourSectionDictionary2(this.conf.getSpec(), subjects, predicates, objects, shared);
		this.saveDictionary(this.dictionary, this.dictionaryFS, this.conf.getDictionaryFile());

		return true;

	}

	protected boolean runTriplesJobSampling() throws ClassNotFoundException, IOException, InterruptedException {
		Job job = null;
		boolean jobOK;
		BufferedWriter bufferedWriter;

		// if input path does not exists, fail
		if (!this.inputFS.exists(this.conf.getInputPath())) {
			System.out.println("Dictionary input path does not exist: " + this.conf.getInputPath());
			System.exit(-1);
		}

		// if dictionary output path does not exists, fail
		if (!this.dictionaryFS.exists(this.conf.getInputPath())) {
			System.out.println("Dictionary output path does not exist: " + this.conf.getInputPath());
			System.exit(-1);
		}

		// if samples path exists, fail
		if (this.dictionaryFS.exists(this.conf.getTriplesSamplesPath())) {
			if (this.conf.getDeleteTriplesSamplesPath()) { // ... and option
				// provided, delete
				// recursively
				this.dictionaryFS.delete(this.conf.getTriplesSamplesPath(), true);
			} else { // ... and option not provided, fail
				System.out.println("Triples samples path does exist: " + this.conf.getTriplesSamplesPath());
				System.out.println("Select other path or use option -dst to overwrite");
				System.exit(-1);
			}
		}

		this.conf.setProperty("mapred.child.java.opts", "-XX:ErrorFile=/home/hadoop/tmp/hs_err_pid%p.log -Xmx2500m");

		// Job to create a SequenceInputFormat
		job = new Job(this.conf.getConfigurationObject(), this.conf.getTriplesJobName() + " phase 1");

		job.setJarByClass(HDTBuilderDriver.class);

		FileInputFormat.addInputPath(job, this.conf.getInputPath());
		FileOutputFormat.setOutputPath(job, this.conf.getTriplesSamplesPath());

		job.setInputFormatClass(LzoTextInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

		job.setMapperClass(TriplesSPOMapper.class);
		job.setSortComparatorClass(TripleSPOComparator.class);
		job.setGroupingComparatorClass(TripleSPOComparator.class);
		job.setMapOutputKeyClass(TripleSPOWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(TripleSPOWritable.class);
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(this.conf.getTriplesReducers());

		DistributedCache.addCacheFile(this.conf.getDictionaryFile().toUri(), job.getConfiguration());

		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, com.hadoop.compression.lzo.LzoCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

		jobOK = job.waitForCompletion(true);

		this.numTriples = job.getCounters().findCounter(Counters.Triples).getValue();
		bufferedWriter = new BufferedWriter(new OutputStreamWriter(this.triplesFS.create(this.conf.getTriplesCountersFile())));
		bufferedWriter.write(this.numTriples.toString() + "\n");
		bufferedWriter.close();

		return jobOK;
	}

	protected boolean runTriplesJob() throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Job job = null;
		boolean jobOK;

		// if triples output path exists...
		if (this.triplesFS.exists(this.conf.getTriplesOutputPath())) {
			if (this.conf.getDeleteTriplesOutputPath()) { // ... and option provided, delete recursively
				this.triplesFS.delete(this.conf.getTriplesOutputPath(), true);
			} else { // ... and option not provided, fail
				System.out.println("Triples output path does exist: " + this.conf.getTriplesOutputPath());
				System.out.println("Select other path or use option -dt to overwrite");
				System.exit(-1);
			}
		}

		job = new Job(this.conf.getConfigurationObject(), this.conf.getTriplesJobName() + " phase 2");

		job.setJarByClass(HDTBuilderDriver.class);

		FileInputFormat.addInputPath(job, this.conf.getTriplesSamplesPath());
		FileOutputFormat.setOutputPath(job, this.conf.getTriplesOutputPath());

		job.setInputFormatClass(SequenceFileInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

		job.setSortComparatorClass(TripleSPOComparator.class);
		job.setGroupingComparatorClass(TripleSPOComparator.class);

		job.setPartitionerClass(TotalOrderPartitioner.class);

		job.setOutputKeyClass(TripleSPOWritable.class);
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(this.conf.getTriplesReducers());

		System.out.println("Sampling started");
		InputSampler.writePartitionFile(job, new InputSampler.IntervalSampler<Text, Text>(this.conf.getSampleProbability()));
		String partitionFile = TotalOrderPartitioner.getPartitionFile(job.getConfiguration());
		URI partitionUri = new URI(partitionFile + "#" + TotalOrderPartitioner.DEFAULT_PATH);
		DistributedCache.addCacheFile(partitionUri, job.getConfiguration());
		DistributedCache.createSymlink(job.getConfiguration());
		System.out.println("Sampling finished");

		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, com.hadoop.compression.lzo.LzoCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

		jobOK = job.waitForCompletion(true);

		return jobOK;
	}

	protected boolean runTriplesJobWithOneJob() throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Job job = null;
		boolean jobOK;
		BufferedWriter bufferedWriter;

		// if input path does not exists, fail
		if (!this.inputFS.exists(this.conf.getInputPath())) {
			System.out.println("Dictionary input path does not exist: " + this.conf.getInputPath());
			System.exit(-1);
		}

		// if dictionary output path does not exists, fail
		if (!this.dictionaryFS.exists(this.conf.getInputPath())) {
			System.out.println("Dictionary output path does not exist: " + this.conf.getInputPath());
			System.exit(-1);
		}

		// if triples output path exists...
		if (this.triplesFS.exists(this.conf.getTriplesOutputPath())) {
			if (this.conf.getDeleteTriplesOutputPath()) { // ... and option provided, delete recursively
				this.triplesFS.delete(this.conf.getTriplesOutputPath(), true);
			} else { // ... and option not provided, fail
				System.out.println("Triples output path does exist: " + this.conf.getTriplesOutputPath());
				System.out.println("Select other path or use option -dt to overwrite");
				System.exit(-1);
			}
		}

		// Launch job
		this.conf.setProperty("mapred.child.java.opts", "-XX:ErrorFile=/home/hadoop/tmp/hs_err_pid%p.log -Xmx2500m");

		job = new Job(this.conf.getConfigurationObject(), this.conf.getDictionaryJobName());
		job.setJarByClass(HDTBuilderDriver.class);

		FileInputFormat.addInputPath(job, this.conf.getInputPath());
		FileOutputFormat.setOutputPath(job, this.conf.getTriplesOutputPath());

		job.setInputFormatClass(LzoTextInputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

		job.setMapperClass(TriplesSPOMapper.class);
		job.setSortComparatorClass(TripleSPOComparator.class);
		job.setMapOutputKeyClass(TripleSPOWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(this.conf.getTriplesReducers());

		job.setOutputKeyClass(TripleSPOWritable.class);
		job.setOutputValueClass(NullWritable.class);

		DistributedCache.addCacheFile(this.conf.getDictionaryFile().toUri(), job.getConfiguration());
		// DistributedCache.addCacheFile(this.conf.getDictionaryMapFile().toUri(), job.getConfiguration());
		// DistributedCache.addCacheFile(this.conf.getDictionaryReduceFile().toUri(), job.getConfiguration());

		jobOK = job.waitForCompletion(true);

		this.numTriples = job.getCounters().findCounter(Counters.Triples).getValue();
		bufferedWriter = new BufferedWriter(new OutputStreamWriter(this.triplesFS.create(this.conf.getTriplesCountersFile())));
		bufferedWriter.write(this.numTriples.toString() + "\n");
		bufferedWriter.close();

		return jobOK;
	}

	protected boolean buidHDT() throws IOException {
		BufferedOutputStream output;
		TransientHDT hdt = new TransientHDT(this.conf.getSpec());
		TransientBitMapTriples triples = new TransientBitMapTriples(this.conf.getSpec(), this.triplesFS, new Path("temp"));

		// if dictionary not built, load it
		if (this.dictionary == null) {
			System.out.println("Dictionary not built. Reading data from " + this.conf.getDictionaryFile());
			this.dictionary = this.loadDictionary(this.dictionaryFS, this.conf.getDictionaryFile());
		}

		// if maxvalues not loaded, read Counters
		if (!this.conf.runDictionary()) {

			System.out.println("Dictionary Samples job not ran. Reading data from file.");

			BufferedReader reader = new BufferedReader(new InputStreamReader(this.dictionaryFS.open(this.conf.getDictionaryCountersFile())));
			String line = reader.readLine();
			while (line != null) {
				String[] data = line.split("=");
				switch (data[0]) {
					case HDTBuilderConfiguration.SHARED:
						this.numShared = Long.parseLong(data[1]);
						break;
					case HDTBuilderConfiguration.SUBJECTS:
						this.numSubjects = Long.parseLong(data[1]);
						break;
					case HDTBuilderConfiguration.PREDICATES:
						this.numPredicates = Long.parseLong(data[1]);
						break;
					case HDTBuilderConfiguration.OBJECTS:
						this.numObjects = Long.parseLong(data[1]);
				}
				line = reader.readLine();
			}
			reader.close();
		}

		// if triples job not ran, read Counters
		if (!this.conf.runTriples()) {
			System.out.println("Triples job nor ran. Reading data from " + this.conf.getTriplesCountersFile());
			BufferedReader reader = new BufferedReader(new InputStreamReader(this.dictionaryFS.open(this.conf.getTriplesCountersFile())));
			this.numTriples = Long.parseLong(reader.readLine());
			reader.close();
		}

		this.loadFromDir(triples, this.numTriples, this.numPredicates, (this.numShared + this.numObjects), this.triplesFS, this.conf.getTriplesOutputPath());

		hdt.setDictionary(this.dictionary);
		hdt.setTriples(triples);
		hdt.populateHeaderStructure(this.conf.getBaseURI());

		output = new BufferedOutputStream(this.triplesFS.create(this.conf.getHDTFile()));
		hdt.saveToHDT(output, this.listener);
		output.close();

		return true;
	}

	protected void loadFromDir(TransientElement part, long numentries, FileSystem fs, Path path) throws IOException {
		PathFilter filter = new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return !path.getName().startsWith("_");
			}
		};
		FileStatus[] status = fs.listStatus(path, filter);

		if (status.length == 0) {
			System.out.println("Path [" + path + "] has no files. Initializing section.");
			part.initialize(0);
		} else {
			Arrays.sort(status, new FileStatusComparator());

			System.out.println("Initializing section " + path);
			part.initialize(numentries);
			for (FileStatus file : status) {
				System.out.println("Reading file [" + file.getPath() + "]");
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, file.getPath(), this.conf.getConfigurationObject());
				part.load(reader, this.listener);
				reader.close();
			}
			System.out.println("Closing section " + path);
			part.close();
		}
	}

	protected void loadFromDir(TransientBitMapTriples part, long numentries, long maxpredicate, long maxobject, FileSystem fs, Path path) throws IOException {
		PathFilter filter = new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return !path.getName().startsWith("_");
			}
		};
		FileStatus[] status = fs.listStatus(path, filter);

		if (status.length == 0) {
			System.out.println("Path [" + path + "] has no files. Initializing section.");
			part.initialize(0, 0);
		} else {
			Arrays.sort(status, new FileStatusComparator());

			System.out.println("Initializing section " + path);
			part.initialize(numentries, maxpredicate, maxobject);
			for (FileStatus file : status) {
				System.out.println("Reading file [" + file.getPath() + "]");
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, file.getPath(), this.conf.getConfigurationObject());
				part.load(reader, this.listener);
				reader.close();
			}
			System.out.println("Closing section " + path);
			part.close();
		}
	}

	protected FourSectionDictionary2 loadDictionary(FileSystem fs, Path dictionaryPath) throws IOException {
		BufferedInputStream input = new BufferedInputStream(fs.open(dictionaryPath));
		FourSectionDictionary2 dictionary = new FourSectionDictionary2(this.conf.getSpec());
		ControlInformation ci = new ControlInformation();
		ci.clear();
		ci.load(input);
		dictionary.load(input, ci, this.listener);
		return dictionary;
	}

	protected void saveDictionary(FourSectionDictionary dictionary, FileSystem fs, Path dictionaryPath) throws IOException {
		BufferedOutputStream output = new BufferedOutputStream(fs.create(dictionaryPath));
		dictionary.save(output, new ControlInformation(), this.listener);
		output.close();
	}

}
