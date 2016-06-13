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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.triples.TripleString;

public class DictionarySamplerMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	TripleString triple = new TripleString();
	try {
	    triple.read(value.toString());
	} catch (ParserException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	context.write(new Text(triple.getSubject().toString()), new Text("S"));
	context.write(new Text(triple.getPredicate().toString()), new Text("P"));
	context.write(new Text(triple.getObject().toString()), new Text("O"));
    }
}
