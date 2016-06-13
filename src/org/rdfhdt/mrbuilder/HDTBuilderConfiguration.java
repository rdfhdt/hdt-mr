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

import java.io.IOException;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.rdfhdt.hdt.options.HDTSpecification;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class HDTBuilderConfiguration {

	public final static int		CHUNK_SIZE										= 1 * 1024 * 1024;

	public final static String	SHARED											= "shared";
	public final static String	SUBJECTS										= "subjects";
	public final static String	PREDICATES										= "predicates";
	public final static String	OBJECTS											= "objects";
	public final static String	SAMPLE											= "samples";

	public final static String	SHARED_OUTPUT_PATH								= SHARED + "/";
	public final static String	SUBJECTS_OUTPUT_PATH							= SUBJECTS + "/";
	public final static String	PREDICATES_OUTPUT_PATH							= PREDICATES + "/";
	public final static String	OBJECTS_OUTPUT_PATH								= OBJECTS + "/";
	public final static String	SAMPLE_OUTPUT_PATH								= SAMPLE + "/";

	final static String			DEFAULT_CONFIGURATION_PATH						= "HDTMRBuilder.xml";

	final static String			AWS_BUCKET_NAME									= "global.bucket";
	final static String			AWS_BUCKET_DEFAULT_VALUE						= null;

	final static String			BASE_PATH_NAME									= "global.path.base";
	final static String			BASE_PATH_DEFAULT_VALUE							= ".";
	final static String			INPUT_PATH_NAME									= "global.path.input";
	final static String			INPUT_PATH_DEFAULT_VALUE						= "input";

	final static String			DICTIONARY_RUN_JOB_NAME							= "job.dictionary.run";
	final static Boolean		DICTIONARY_RUN_JOB_DEFAULT_VALUE				= true;
	final static String			DICTIONARY_JOB_NAME_NAME						= "job.dictionary.name";
	final static String			DICTIONARY_JOB_NAME_DEFAULT_VALUE				= "DictionaryJob";
	final static String			DICTIONARY_OUTPUT_PATH_NAME						= "job.dictionary.path.output";
	final static String			DICTIONARY_OUTPUT_PATH_DEFAULT_VALUE			= "dictionary";
	final static String			DICTIONARY_DELETE_OUTPUT_PATH_NAME				= "job.dictionary.path.output.delete";
	final static boolean		DICTIONARY_DELETE_OUTPUT_PATH_DEFAULT_VALUE		= false;
	final static String			DICTIONARY_NUM_REDUCERS_NAME					= "job.dictionary.reducers";
	final static int			DICTIONARY_NUM_REDUCERS_DEFAULT_VALUE			= 1;

	final static String			DICTIONARY_RUN_SAMPLE_NAME						= "job.dictionary.sample.run";
	final static boolean		DICTIONARY_RUN_SAMPLE_DEFAULT_VALUE				= true;
	final static String			DICTIONARY_SAMPLE_PROBABILITY_NAME				= "job.dictionary.sample.probability";
	final static float			DICTIONARY_SAMPLE_PROBABILITY_DEFAULT_VALUE		= (float) 0.001;
	final static String			DICTIONARY_SAMPLE_OUTPUT_PATH_NAME				= "job.dictionary.path.sample";
	final static String			DICTIONARY_SAMPLE_OUTPUT_PATH_DEFAULT_VALUE		= "dictionary_samples";
	final static String			DICTIONARY_DELETE_SAMPLE_PATH_NAME				= "job.dictionary.path.sample.delete";
	final static boolean		DICTIONARY_DELETE_SAMPLE_PATH_DEFAULT_VALUE		= false;
	final static String			DICTIONARY_SAMPLE_NUM_REDUCERS_NAME				= "job.dictionary.sample.reducers";
	final static int			DICTIONARY_SAMPLE_NUM_REDUCERS_DEFAULT_VALUE	= 1;

	final static String			HDTDICTIONARY_BUILD_NAME						= "hdt.dictionary.build";
	final static boolean		HDTDICTIONARY_BUILD_DEFAULT_VALUE				= true;
	final static String			HDTDICTIONARY_FILE_NAME							= "hdt.dictionary.file";
	final static String			HDTDICTIONARY_FILE_DEFAULT_VALUE				= "dictionary.hdt";
	final static String			HDTDICTIONARY_DISTRIBUTION_NAME					= "job.triples.dictionary.distribution";
	final static int			HDTDICTIONARY_DISTRIBUTION_DEFAULT_VALUE		= 1;

	final static String			TRIPLES_RUN_JOB_NAME							= "job.triples.run";
	final static boolean		TRIPLES_RUN_JOB_DEFAULT_VALUE					= true;
	final static String			TRIPLES_JOB_NAME_NAME							= "job.triples.name";
	final static String			TRIPLES_JOB_NAME_DEFAULT_VALUE					= "TriplesJob";
	// final static String TRIPLES_MAP_DICTIONARY_FILE_NAME = "job.triples.map.dictionary.file";
	// final static String TRIPLES_MAP_DICTIONARY_FILE_DEFAULT_VALUE = "dictionary_map.hdt";
	// final static String TRIPLES_REDUCE_DICTIONARY_FILE_NAME = "job.triples.reduce.dictionary.file";
	// final static String TRIPLES_REDUCE_DICTIONARY_FILE_DEFAULT_VALUE = "dictionary_reduce.hdt";
	final static String			TRIPLES_OUTPUT_PATH_NAME						= "job.triples.path.output";
	final static String			TRIPLES_OUTPUT_PATH_DEFAULT_VALUE				= "triples";
	final static String			TRIPLES_DELETE_OUTPUT_PATH_NAME					= "job.triples.path.output.delete";
	final static boolean		TRIPLES_DELETE_OUTPUT_PATH_DEFAULT_VALUE		= false;
	final static String			TRIPLES_NUM_REDUCERS_NAME						= "job.triples.reducers";
	final static int			TRIPLES_NUM_REDUCERS_DEFAULT_VALUE				= 1;

	final static String			TRIPLES_RUN_SAMPLE_NAME							= "job.triples.sample.run";
	final static boolean		TRIPLES_RUN_SAMPLE_DEFAULT_VALUE				= true;
	final static String			TRIPLES_SAMPLE_PROBABILITY_NAME					= "job.triples.sample.probability";
	final static float			TRIPLES_SAMPLE_PROBABILITY_DEFAULT_VALUE		= (float) 0.001;
	final static String			TRIPLES_SAMPLE_OUTPUT_PATH_NAME					= "job.triples.path.sample";
	final static String			TRIPLES_SAMPLE_OUTPUT_PATH_DEFAULT_VALUE		= "triples_samples";
	final static String			TRIPLES_DELETE_SAMPLE_PATH_NAME					= "job.triples.path.sample.delete";
	final static boolean		TRIPLES_DELETE_SAMPLE_PATH_DEFAULT_VALUE		= false;
	final static String			TRIPLES_SAMPLE_NUM_REDUCERS_NAME				= "job.triples.sample.reducers";
	final static int			TRIPLES_SAMPLE_NUM_REDUCERS_DEFAULT_VALUE		= 1;

	final static String			HDT_BUILD_NAME									= "hdt.build";
	final static boolean		HDT_BUILD_DEFAULT_VALUE							= true;
	final static String			HDT_OUTPUT_PATH_NAME							= "hdt.path.output";
	final static String			HDT_OUTPUT_PATH_DEFAULT_VALUE					= "hdt_output";
	final static String			HDT_FILE_NAME									= "hdt.file";
	final static String			HDT_FILE_DEFAULT_VALUE							= "output.hdt";

	final static String			CONFIG_FILE_NAME								= "hdt-lib.configFile";
	final static String			CONFIG_FILE_DEFAULT_VALUE						= null;
	final static String			OPTIONS_NAME									= "hdtl-lib.options";
	final static String			OPTIONS_DEFAULT_VALUE							= null;
	final static String			RDF_TYPE_NAME									= "hdt-lib.rdfType";
	final static String			RDF_TYPE_DEFAULT_VALUE							= "ntriples";
	final static String			QUIET_NAME										= "hdt-lib.quiet";
	final static boolean		QUIET_DEFAULT_VALUE								= false;
	final static String			BASE_URI_NAME									= "hdt-lib.baseUri";
	final static String			BASE_URI_DEFAULT_VALUE							= "http://rdfhdt.org/HDTMR";
	final static String			GENERATE_INDEX_NAME								= "hdt-lib.generateIndex";
	final static boolean		GENERATE_INDEX_DEFAULT_VALUE					= false;

	JCommander					jc;

	@Parameter(names = { "-h", "--help" }, help = true, hidden = true)
	boolean						help											= false;

	@Parameter(names = { "-a", "--awsbucket" }, description = "Amazon Web Services bucket")
	String						pAwsBucket										= null;

	@Parameter(names = { "-c", "--conf" }, description = "Path to configuration file")
	String						pConfigFile										= null;

	@Parameter(names = { "-b", "--basedir" }, description = "Root directory for the process")
	String						pBasePath										= null;

	@Parameter(names = { "-rd", "--rundictionary" }, description = "Whether to run dictionary job or not", arity = 1)
	Boolean						pRunDictionary									= null;

	@Parameter(names = { "-rds", "--rundictionarysampling" }, description = "Whether to run dictionary input sampling job or not", arity = 1)
	Boolean						pRunDictionarySampling							= null;

	@Parameter(names = { "-nd", "--namedictionaryjob" }, description = "Name of dictionary job")
	String						pDictionaryName									= null;

	@Parameter(names = { "-i", "--input" }, description = "Path to input files. Relative to basedir")
	String						pInputPath										= null;

	@Parameter(names = { "-sd", "--samplesdictionary" }, description = "Path to dictionary job sample files. Relative to basedir")
	String						pDictionarySamplePath							= null;

	@Parameter(names = { "-st", "--samplestriples" }, description = "Path to triples job sample files. Relative to basedir")
	String						pTriplesSamplePath								= null;

	@Parameter(names = { "-od", "--outputdictionary" }, description = "Path to dictionary job output files. Relative to basedir")
	String						pDictionaryOutputPath							= null;

	@Parameter(names = { "-dd", "--deleteoutputdictionary" }, description = "Delete dictionary job output path before running job")
	Boolean						pDeleteDictionaryOutputPath						= null;

	@Parameter(names = { "-dsd", "--deletesampledictionary" }, description = "Delete dictionary job sample path before running job")
	Boolean						pDeleteDictionarySamplePath						= null;

	@Parameter(names = { "-dst", "--deletesampletriples" }, description = "Delete triples job sample path before running job")
	Boolean						pDeleteTriplesSamplePath						= null;

	@Parameter(names = { "-Rd", "--reducersdictionary" }, description = "Number of reducers for dictionary job")
	Integer						pNumReducersDictionary							= null;

	@Parameter(names = { "-Rds", "--reducersdictionarysampling" }, description = "Number of reducers for dictionary input sampling job")
	Integer						pNumReducersDictionarySampling					= null;

	@Parameter(names = { "-bd", "--builddictionary" }, description = "Whether to build HDT dictionary or not", arity = 1)
	Boolean						pBuildDictionary								= null;

	@Parameter(names = { "-bh", "--buildhdt" }, description = "Whether to build HDT or not", arity = 1)
	Boolean						pBuildHDT										= null;

	@Parameter(names = { "-fd", "--filedictionary" }, description = "Name of hdt dictionary file")
	String						pDictionaryFileName								= null;

	@Parameter(names = { "-fm", "--filesubjects" }, description = "Name of hdt dictionary file for Mappers")
	String						pMapDictionaryFileName							= null;

	@Parameter(names = { "-fr", "--fileobjects" }, description = "Name of hdt dictionary file for Reducers")
	String						pReduceDictionaryFileName						= null;

	@Parameter(names = { "-d", "--dictionarydistribution" }, description = "Dictionary distribution among mappers and reducers")
	Integer						pDictionaryDistribution							= null;

	@Parameter(names = { "-rt", "--runtriples" }, description = "Whether to run triples job or not", arity = 1)
	Boolean						pRunTriples										= null;

	@Parameter(names = { "-rts", "--runtriplessampling" }, description = "Whether to run triples input sampling job or not", arity = 1)
	Boolean						pRunTriplesSampling								= null;

	@Parameter(names = { "-nt", "--nametriplesjob" }, description = "Name of triples job")
	String						pTriplesName									= null;

	@Parameter(names = { "-it", "--inputtriples" }, description = "Path to triples job input files. Relative to basedir")
	String						pTriplesInputPath								= null;

	@Parameter(names = { "-ot", "--outputtriples" }, description = "Path to triples job output files. Relative to basedir")
	String						pTriplesOutputPath								= null;

	@Parameter(names = { "-dt", "--deleteoutputtriples" }, description = "Delete triples job output path before running job")
	Boolean						pDeleteTriplesOutputPath						= null;

	@Parameter(names = { "-Rt", "--reducerstriples" }, description = "Number of reducers for triples job")
	Integer						pNumReducersTriples								= null;

	@Parameter(names = { "-Rts", "--reducerstriplessampling" }, description = "Number of reducers for triples input sampling job")
	Integer						pNumReducersTriplesSampling						= null;

	@Parameter(names = { "-fh", "--namehdtfile" }, description = "Name of hdt  file")
	String						pHdtFileName									= null;

	@Parameter(names = { "-hc", "--hdtconf" }, description = "Conversion config file")
	String						pHdtConfigFile									= null;

	@Parameter(names = { "-o", "--options" }, description = "HDT Conversion options (override those of config file)")
	String						pOptions										= null;

	@Parameter(names = { "-t", "--rdftype" }, description = "Type of RDF Input (ntriples, nquad, n3, turtle, rdfxml)")
	String						pRdfType										= null;

	@Parameter(names = { "-bu", "--baseURI" }, description = "Base URI for the dataset")
	String						pBaseURI										= null;

	@Parameter(names = { "-q", "--quiet" }, description = "Do not show progress of the conversion")
	Boolean						pQuiet											= null;

	@Parameter(names = { "-x", "--index" }, description = "Generate also external indices to solve all queries")
	Boolean						pGenerateIndex									= null;

	@Parameter(names = { "-p", "--sampleprobability" }, description = "Probability of using each element for sampling")
	Float						pSampleProbability								= null;

	Path						inputPath										= null, dictionarySamplesPath = null, dictionaryOutputPath = null, sharedOutputPath = null, subjectsOutputPath = null, predicatesOutputPath = null, objectsOutputPath = null;
	Path						dictionaryCountersFile							= null, triplesSamplesPath = null, triplesCountersFile = null, hdtDictionarySPOFile = null, hdtMapDictionaryFile = null, hdtReduceDictionaryFile = null, hdtFile = null;
	Path						triplesInputPath								= null, triplesOutputPath = null;

	Configuration				mrConfiguration									= new Configuration();

	HDTSpecification			spec;

	// This constructor is to be used by Tasks (Mappers and/or Reducers)
	public HDTBuilderConfiguration(Configuration config) throws IOException {
		this.mrConfiguration = config;
	}

	// This constructor is to be used by Drivers
	public HDTBuilderConfiguration(String[] args) {
		this.jc = new JCommander(this, args);
		if (this.help) {
			this.jc.usage();
			System.exit(1);
		}
		this.addConfigurationResource(this.getConfigFile());

		// FIXME: Esto debería hacerse para todos los parámetros pasados por
		// línea de comandos
		this.setProperty(DICTIONARY_OUTPUT_PATH_NAME, this.getDictionaryOutputPath().toString());
	}

	private void addConfigurationResource(String configurationPath) {
		this.mrConfiguration.addResource(new Path(configurationPath));
	}

	private String getConfigFile() {
		return this.addBucket(this.pConfigFile != null ? this.pConfigFile : DEFAULT_CONFIGURATION_PATH);
	}

	public Configuration getConfigurationObject() {
		return this.mrConfiguration;
	}

	public void setProperty(String name, String value) {
		this.mrConfiguration.set(name, value);
	}

	public void setProperty(String name, int value) {
		this.mrConfiguration.setInt(name, value);
	}

	public String getAwsBucket() {
		return this.get(this.pAwsBucket, AWS_BUCKET_NAME, AWS_BUCKET_DEFAULT_VALUE);
	}

	public boolean runDictionary() {
		return this.get(this.pRunDictionary, DICTIONARY_RUN_JOB_NAME, DICTIONARY_RUN_JOB_DEFAULT_VALUE);
	}

	public boolean runDictionarySampling() {
		return this.get(this.pRunDictionarySampling, DICTIONARY_RUN_SAMPLE_NAME, DICTIONARY_RUN_SAMPLE_DEFAULT_VALUE);
	}

	public boolean runTriples() {
		return this.get(this.pRunTriples, TRIPLES_RUN_JOB_NAME, TRIPLES_RUN_JOB_DEFAULT_VALUE);
	}

	public boolean runTriplesSampling() {
		return this.get(this.pRunTriplesSampling, TRIPLES_RUN_SAMPLE_NAME, TRIPLES_RUN_SAMPLE_DEFAULT_VALUE);
	}

	public boolean buildDictionary() {
		return this.get(this.pBuildDictionary, HDTDICTIONARY_BUILD_NAME, HDTDICTIONARY_BUILD_DEFAULT_VALUE);
	}

	public boolean buildHDT() {
		return this.get(this.pBuildHDT, HDT_BUILD_NAME, HDT_BUILD_DEFAULT_VALUE);
	}

	public String getDictionaryJobName() {
		return this.get(this.pTriplesName, DICTIONARY_JOB_NAME_NAME, DICTIONARY_JOB_NAME_DEFAULT_VALUE);
	}

	public String getTriplesJobName() {
		return this.get(this.pTriplesName, DICTIONARY_JOB_NAME_NAME, DICTIONARY_JOB_NAME_DEFAULT_VALUE);
	}

	public Path getInputPath() {
		if (this.inputPath == null) {
			this.inputPath = new Path(this.getPath(this.get(this.pInputPath, INPUT_PATH_NAME, INPUT_PATH_DEFAULT_VALUE)));
		}
		return this.inputPath;
	}

	public Path getDictionaryOutputPath() {
		if (this.dictionaryOutputPath == null) {
			this.dictionaryOutputPath = new Path(this.getPath(this.get(this.pDictionaryOutputPath, DICTIONARY_OUTPUT_PATH_NAME, DICTIONARY_OUTPUT_PATH_DEFAULT_VALUE)));
		}
		return this.dictionaryOutputPath;
	}

	public Path getSharedSectionPath() {
		if (this.sharedOutputPath == null) {
			this.sharedOutputPath = new Path(this.getPath(this.get(this.pDictionaryOutputPath, DICTIONARY_OUTPUT_PATH_NAME, DICTIONARY_OUTPUT_PATH_DEFAULT_VALUE)) + "/" + SHARED_OUTPUT_PATH);
		}
		return this.sharedOutputPath;
	}

	public Path getSubjectsSectionPath() {
		if (this.subjectsOutputPath == null) {
			this.subjectsOutputPath = new Path(this.getPath(this.get(this.pDictionaryOutputPath, DICTIONARY_OUTPUT_PATH_NAME, DICTIONARY_OUTPUT_PATH_DEFAULT_VALUE)) + "/" + SUBJECTS_OUTPUT_PATH);
		}
		return this.subjectsOutputPath;
	}

	public Path getPredicatesSectionPath() {
		if (this.predicatesOutputPath == null) {
			this.predicatesOutputPath = new Path(this.getPath(this.get(this.pDictionaryOutputPath, DICTIONARY_OUTPUT_PATH_NAME, DICTIONARY_OUTPUT_PATH_DEFAULT_VALUE)) + "/" + PREDICATES_OUTPUT_PATH);
		}
		return this.predicatesOutputPath;
	}

	public Path getObjectsSectionPath() {
		if (this.objectsOutputPath == null) {
			this.objectsOutputPath = new Path(this.getPath(this.get(this.pDictionaryOutputPath, DICTIONARY_OUTPUT_PATH_NAME, DICTIONARY_OUTPUT_PATH_DEFAULT_VALUE)) + "/" + OBJECTS_OUTPUT_PATH);
		}
		return this.objectsOutputPath;
	}

	public Path getDictionarySamplesPath() {
		if (this.dictionarySamplesPath == null) {
			this.dictionarySamplesPath = new Path(this.getPath(this.get(this.pDictionarySamplePath, DICTIONARY_SAMPLE_OUTPUT_PATH_NAME, DICTIONARY_SAMPLE_OUTPUT_PATH_DEFAULT_VALUE)));
		}
		return this.dictionarySamplesPath;
	}

	public Path getTriplesSamplesPath() {
		if (this.triplesSamplesPath == null) {
			this.triplesSamplesPath = new Path(this.getPath(this.get(this.pTriplesSamplePath, TRIPLES_SAMPLE_OUTPUT_PATH_NAME, TRIPLES_SAMPLE_OUTPUT_PATH_DEFAULT_VALUE)));
		}
		return this.triplesSamplesPath;
	}

	public float getSampleProbability() {
		return this.get(this.pSampleProbability, DICTIONARY_SAMPLE_PROBABILITY_NAME, DICTIONARY_SAMPLE_PROBABILITY_DEFAULT_VALUE);
	}

	public Path getDictionaryCountersFile() {
		if (this.dictionaryCountersFile == null) {
			this.dictionaryCountersFile = new Path(this.getPath(this.get(this.pDictionaryOutputPath, DICTIONARY_OUTPUT_PATH_NAME, DICTIONARY_OUTPUT_PATH_DEFAULT_VALUE)) + ".info");
		}
		return this.dictionaryCountersFile;
	}

	public Path getDictionaryFile() {
		if (this.hdtDictionarySPOFile == null) {
			this.hdtDictionarySPOFile = new Path(this.getPath(this.get(this.pDictionaryOutputPath, DICTIONARY_OUTPUT_PATH_NAME, DICTIONARY_OUTPUT_PATH_DEFAULT_VALUE)) + "/" + this.get(this.pDictionaryFileName, HDTDICTIONARY_FILE_NAME, HDTDICTIONARY_FILE_DEFAULT_VALUE));
		}
		return this.hdtDictionarySPOFile;
	}

	// public Path getDictionaryMapFile() {
	// if (this.hdtMapDictionaryFile == null) {
	// this.hdtMapDictionaryFile = new Path(this.getPath(this.get(this.pDictionaryOutputPath, DICTIONARY_OUTPUT_PATH_NAME, DICTIONARY_OUTPUT_PATH_DEFAULT_VALUE)) + "/" + this.get(this.pMapDictionaryFileName, TRIPLES_MAP_DICTIONARY_FILE_NAME, TRIPLES_MAP_DICTIONARY_FILE_DEFAULT_VALUE));
	// }
	// return this.hdtMapDictionaryFile;
	// }
	//
	// public Path getDictionaryReduceFile() {
	// if (this.hdtReduceDictionaryFile == null) {
	// this.hdtReduceDictionaryFile = new Path(this.getPath(this.get(this.pDictionaryOutputPath, DICTIONARY_OUTPUT_PATH_NAME, DICTIONARY_OUTPUT_PATH_DEFAULT_VALUE)) + "/" + this.get(this.pReduceDictionaryFileName, TRIPLES_REDUCE_DICTIONARY_FILE_NAME, TRIPLES_REDUCE_DICTIONARY_FILE_DEFAULT_VALUE));
	// }
	// return this.hdtReduceDictionaryFile;
	// }

	public int getDictionaryDistribution() {
		return this.get(this.pDictionaryDistribution, HDTDICTIONARY_DISTRIBUTION_NAME, HDTDICTIONARY_DISTRIBUTION_DEFAULT_VALUE);
	}

	public Path getTriplesOutputPath() {
		if (this.triplesOutputPath == null) {
			this.triplesOutputPath = new Path(this.getPath(this.get(this.pTriplesOutputPath, TRIPLES_OUTPUT_PATH_NAME, TRIPLES_OUTPUT_PATH_DEFAULT_VALUE)));
		}
		return this.triplesOutputPath;
	}

	public Path getTriplesCountersFile() {
		if (this.triplesCountersFile == null) {
			this.triplesCountersFile = new Path(this.getPath(this.get(this.pTriplesOutputPath, TRIPLES_OUTPUT_PATH_NAME, TRIPLES_OUTPUT_PATH_DEFAULT_VALUE)) + ".info");
		}
		return this.triplesCountersFile;
	}

	public Path getHDTFile() {
		if (this.hdtFile == null) {
			this.hdtFile = new Path(this.getPath(this.get(this.pHdtFileName, HDT_FILE_NAME, HDT_FILE_DEFAULT_VALUE)));
		}
		return this.hdtFile;
	}

	public boolean getDeleteDictionaryOutputPath() {
		return this.get(this.pDeleteDictionaryOutputPath, DICTIONARY_DELETE_OUTPUT_PATH_NAME, DICTIONARY_DELETE_OUTPUT_PATH_DEFAULT_VALUE);
	}

	public boolean getDeleteDictionarySamplesPath() {
		return this.get(this.pDeleteDictionarySamplePath, DICTIONARY_DELETE_SAMPLE_PATH_NAME, DICTIONARY_DELETE_SAMPLE_PATH_DEFAULT_VALUE);
	}

	public boolean getDeleteTriplesOutputPath() {
		return this.get(this.pDeleteTriplesOutputPath, TRIPLES_DELETE_OUTPUT_PATH_NAME, TRIPLES_DELETE_OUTPUT_PATH_DEFAULT_VALUE);
	}

	public boolean getDeleteTriplesSamplesPath() {
		return this.get(this.pDeleteTriplesSamplePath, TRIPLES_DELETE_SAMPLE_PATH_NAME, TRIPLES_DELETE_SAMPLE_PATH_DEFAULT_VALUE);
	}

	public int getDictionaryReducers() {
		return this.get(this.pNumReducersDictionary, DICTIONARY_NUM_REDUCERS_NAME, DICTIONARY_NUM_REDUCERS_DEFAULT_VALUE);
	}

	public int getDictionarySampleReducers() {
		return this.get(this.pNumReducersDictionarySampling, DICTIONARY_SAMPLE_NUM_REDUCERS_NAME, DICTIONARY_SAMPLE_NUM_REDUCERS_DEFAULT_VALUE);
	}

	public int getTriplesReducers() {
		return this.get(this.pNumReducersTriples, TRIPLES_NUM_REDUCERS_NAME, TRIPLES_NUM_REDUCERS_DEFAULT_VALUE);
	}

	public int getTriplesSampleReducers() {
		return this.get(this.pNumReducersTriplesSampling, TRIPLES_SAMPLE_NUM_REDUCERS_NAME, TRIPLES_SAMPLE_NUM_REDUCERS_DEFAULT_VALUE);
	}

	public String getHdtConfigFile() {
		return this.getPath(this.get(this.pHdtConfigFile, CONFIG_FILE_NAME, CONFIG_FILE_DEFAULT_VALUE));
	}

	public String getOptions() {
		return this.get(this.pOptions, OPTIONS_NAME, OPTIONS_DEFAULT_VALUE);
	}

	public String getRdfType() {
		return this.get(this.pRdfType, RDF_TYPE_NAME, RDF_TYPE_DEFAULT_VALUE);
	}

	public boolean getQuiet() {
		return this.get(this.pQuiet, QUIET_NAME, QUIET_DEFAULT_VALUE);
	}

	public String getBaseURI() {
		return this.get(this.pBaseURI, BASE_URI_NAME, BASE_URI_DEFAULT_VALUE);
	}

	public HDTSpecification getSpec() throws IOException {
		if (this.spec == null) {
			if (this.getHdtConfigFile() != null) {
				this.spec = new HDTSpecification(this.getHdtConfigFile());
			} else {
				this.spec = new HDTSpecification();
			}
			if (this.getOptions() != null) {
				this.spec.setOptions(this.getOptions());
			}
		}
		return this.spec;
	}

	private String get(String paramValue, String confName, String defaultValue) {
		return paramValue != null ? paramValue : this.mrConfiguration.get(confName, defaultValue);
	}

	private boolean get(Boolean paramValue, String confName, boolean defaultValue) {
		return paramValue != null ? paramValue : this.mrConfiguration.getBoolean(confName, defaultValue);
	}

	private int get(Integer paramValue, String confName, int defaultValue) {
		return paramValue != null ? paramValue : this.mrConfiguration.getInt(confName, defaultValue);
	}

	private float get(Float paramValue, String confName, float defaultValue) {
		return paramValue != null ? paramValue : this.mrConfiguration.getFloat(confName, defaultValue);
	}

	private String getPath(String path) {
		// Add Base Path
		return FilenameUtils.concat(this.get(this.pBasePath, BASE_PATH_NAME, BASE_PATH_DEFAULT_VALUE), path);
	}

	private String addBucket(String path) {
		// If bucket is provided as parameter, and configuration path is
		// relative, create absolute configuration path
		if (this.getAwsBucket() != null && !path.startsWith("s3n://")) {
			path = "s3n://" + this.getAwsBucket() + "/" + StringUtils.removeStart(path, "/");
		}
		return path;
	}

	// private void set(Integer paramValue, String confName, int defautlValue) {
	// mrConfiguration.setInt(confName, paramValue != null ? paramValue :
	// mrConfiguration.getInt(confName, defautlValue));
	// }

}
