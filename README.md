# HDT-MR Library

Copyright (C) 2015, Jose M. Gimenez-Garcia, Javier D. Fernandez,
Miguel A. Martinez-Prieto All rights reserved.

This library is free software; you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as
published by the Free Software Foundation; either version 2.1 of the
License, or (at your option) any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301
USA

Visit our Web Page: https://dataweb.infor.uva.es/projects/hdt-mr

Contacting the authors:
- Jose M. Gimenez-Garcia:    josemiguel.gimenez@alumnos.uva.es
- Javier D. Fernandez:       jfergar@infor.uva.es, javier.fernandez@wu.ac.at
- Miguel A. Martinez-Prieto: migumar2@infor.uva.es


## Overview

HDT-MR improves the HDT-java library by introducing MapReduce as the
computation model for large HDT serialization. HDT-MR performs in
linear time with the dataset size and has proven able to serialize
datasets up to 4.42 billion triples, preserving HDT compression and
retrieval features.

HDT-java is a Java library that implements the W3C Submission
(http://www.w3.org/Submission/2011/03/) of the RDF HDT
(Header-Dictionary-Triples) binary format for publishing and
exchanging RDF data at large scale. Its compact representation allows
storing RDF in fewer space, while providing direct access to the
stored information. See rdfhdt.org for further information.

HDT-MR provides three components:
- iface: Provides an API to use HDT-MR, including interfaces and abstract classes 
- src: Core library and command lines tools for using HDT-MR. It allows creating HDT files from RDF.
- config: Examples of configuration files

Note that the current distribution is an alpha version. Therefore,
while this build has been tested, it is still subject to bugs and
optimizations.

## Compiling

Dependencies: 
- HDT-java (https://github.com/rdfhdt/hdt-java).

src/org/rdfhdt/hdt includes those classes who has been
modified/extended

## Command line tools

The tool provides the following main command line tool:

```
Usage: hadoop HDTBuilderDriver [options]
  Options:
    -a, --awsbucket
       Amazon Web Services bucket
    -bu, --baseURI
       Base URI for the dataset
    -b, --basedir
       Root directory for the process
    -bd, --builddictionary
       Whether to build HDT dictionary or not
    -bh, --buildhdt
       Whether to build HDT or not
    -c, --conf
       Path to configuration file
    -dd, --deleteoutputdictionary
       Delete dictionary job output path before running job
    -dt, --deleteoutputtriples
       Delete triples job output path before running job
    -dsd, --deletesampledictionary
       Delete dictionary job sample path before running job
    -dst, --deletesampletriples
       Delete triples job sample path before running job
    -d, --dictionarydistribution
       Dictionary distribution among mappers and reducers
    -fd, --filedictionary
       Name of hdt dictionary file
    -fr, --fileobjects
       Name of hdt dictionary file for Reducers
    -fm, --filesubjects
       Name of hdt dictionary file for Mappers
    -hc, --hdtconf
       Conversion config file
    -x, --index
       Generate also external indices to solve all queries
    -i, --input
       Path to input files. Relative to basedir
    -it, --inputtriples
       Path to triples job input files. Relative to basedir
    -nd, --namedictionaryjob
       Name of dictionary job
    -fh, --namehdtfile
       Name of hdt  file
    -nt, --nametriplesjob
       Name of triples job
    -o, --options
       HDT Conversion options (override those of config file)
    -od, --outputdictionary
       Path to dictionary job output files. Relative to basedir
    -ot, --outputtriples
       Path to triples job output files. Relative to basedir
    -q, --quiet
       Do not show progress of the conversion
    -t, --rdftype
       Type of RDF Input (ntriples, nquad, n3, turtle, rdfxml)
    -Rd, --reducersdictionary
       Number of reducers for dictionary job
    -Rds, --reducersdictionarysampling
       Number of reducers for dictionary input sampling job
    -Rt, --reducerstriples
       Number of reducers for triples job
    -Rts, --reducerstriplessampling
       Number of reducers for triples input sampling job
    -rd, --rundictionary
       Whether to run dictionary job or not
    -rds, --rundictionarysampling
       Whether to run dictionary input sampling job or not
    -rt, --runtriples
       Whether to run triples job or not
    -rts, --runtriplessampling
       Whether to run triples input sampling job or not
    -p, --sampleprobability
       Probability of using each element for sampling
    -sd, --samplesdictionary
       Path to dictionary job sample files. Relative to basedir
    -st, --samplestriples
       Path to triples job sample files. Relative to basedir
```

## Usage example

After installation, run:

```
$ hadoop HDTBuilderDriver
```

This first try to read configuration parameters at the default config
file (HDTMRBuilder.xml), using default values for those missing
parameters. It reads RDF input data from the default 'input' folder
and outputs the HDT conversion in 'output.hdt'

```
$ hadoop HDTBuilderDriver -i mashup
```

Same previous example, but it reads RDF input data from the directory
'mashup'

```
$ hadoop HDTBuilderDriver -c lubm-dictionary.xml -p 0.01
```

It uses 'lubm-dictionary.xml' as the configuration file. This file
states that input data must be taken from the 'lubm' directory and it
forces to compute only the HDT dictionary, which is written in
'dictionary/dictionary.hdt'

It uses 0.01 as the probability of using each element for sampling.


```
$ hadoop HDTBuilderDriver -c lubm-triples.xml -Rt 1 -Rts 1
```

It uses 'lubm-triples.xml' as the configuration file. This file states
that input data must be taken from the 'lubm' directory and it forces
to compute the HDT triples and the final HDT representation by taken
the already computed dictionary in 'dictionary/dictionary.hdt'

It forces to use one reducer in both jobs.

## License

All HDT-MR content is licensed by Lesser General Public License.

## Acknowledgements

HDT-MR is a project partially funded by Ministerio de Economia y
Competitividad, Spain: TIN2013-46238-C4-3-R, and Austrian Science Fund
(FWF): M1720-G11.
