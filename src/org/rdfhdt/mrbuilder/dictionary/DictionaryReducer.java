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
package org.rdfhdt.mrbuilder.dictionary;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.rdfhdt.mrbuilder.HDTBuilderConfiguration;
import org.rdfhdt.mrbuilder.HDTBuilderDriver.Counters;

public class DictionaryReducer extends Reducer<Text, Text, Text, NullWritable> {

    protected MultipleOutputs<Text, NullWritable> output;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
	this.output = new MultipleOutputs<Text, NullWritable>(context);
	super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	boolean isSubject = false, isPredicate = false, isObject = false;

	//key = new Text(UnicodeEscape.escapeString(key.toString()));

	for (Text value : values) {
	    if (value.toString().contains("S"))
		isSubject = true;
	    if (value.toString().contains("P"))
		isPredicate = true;
	    if (value.toString().contains("O"))
		isObject = true;
	}

	if (isSubject && isObject) {
	    this.output.write(HDTBuilderConfiguration.SHARED, key, NullWritable.get(), HDTBuilderConfiguration.SHARED_OUTPUT_PATH);
	    context.getCounter(Counters.Shared).increment(1);
	} else {
	    if (isSubject) {
		this.output.write(HDTBuilderConfiguration.SUBJECTS, key, NullWritable.get(), HDTBuilderConfiguration.SUBJECTS_OUTPUT_PATH);
		context.getCounter(Counters.Subjects).increment(1);
	    }
	    if (isObject) {
		this.output.write(HDTBuilderConfiguration.OBJECTS, key, NullWritable.get(), HDTBuilderConfiguration.OBJECTS_OUTPUT_PATH);
		context.getCounter(Counters.Objects).increment(1);
	    }
	}
	if (isPredicate) {
	    this.output.write(HDTBuilderConfiguration.PREDICATES, key, NullWritable.get(), HDTBuilderConfiguration.PREDICATES_OUTPUT_PATH);
	    context.getCounter(Counters.Predicates).increment(1);
	}
	
//	if (key.toString().contains("Forest Green is an unincorporated community in southeastern Chariton County"))
//    	System.out.println("Reducer: " + key.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
	this.output.close();
	super.cleanup(context);
    }
}
