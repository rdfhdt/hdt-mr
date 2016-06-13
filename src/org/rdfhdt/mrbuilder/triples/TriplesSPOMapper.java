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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.mrbuilder.io.TripleSPOWritable;

public class TriplesSPOMapper extends TriplesMapper<TripleSPOWritable, NullWritable> {

    /*
     * (non-Javadoc)
     * 
     * @see org.rdfhdt.mrbuilder.triples.TriplesMapper#key(org.rdfhdt.hdt.triples.TripleString)
     */
    @Override
    protected TripleSPOWritable key(TripleString tripleString) throws InterruptedException {
    	long subject, predicate, object;

	if ((subject = this.dictionary.stringToId(tripleString.getSubject(), TripleComponentRole.SUBJECT)) == -1) {
	    System.out.println("Subject nor found");
		System.out.println("Subject [" + tripleString.getSubject() + "]");
	    System.out.println("Predicate [" + tripleString.getPredicate() + "]");
	    System.out.println("Object [" + tripleString.getObject() + "]");
		throw new InterruptedException("Dictionary not loaded correctly");
	}
	if ((predicate = this.dictionary.stringToId(tripleString.getPredicate(), TripleComponentRole.PREDICATE)) == -1)
	{
		System.out.println("Predicate nor found");
		System.out.println("Subject [" + tripleString.getSubject() + "]");
	    System.out.println("Predicate [" + tripleString.getPredicate() + "]");
	    System.out.println("Object [" + tripleString.getObject() + "]");
	    throw new InterruptedException("Dictionary not loaded correctly");
	}
	if ((object = this.dictionary.stringToId(tripleString.getObject(), TripleComponentRole.OBJECT)) == -1)
	{
		System.out.println("Object nor found");
		System.out.println("Subject [" + tripleString.getSubject() + "]");
	    System.out.println("Predicate [" + tripleString.getPredicate() + "]");
	    System.out.println("Object [" + tripleString.getObject() + "]");
	    throw new InterruptedException("Dictionary not loaded correctly");
	}
	
	TripleSPOWritable tripleIDs = new TripleSPOWritable();
	tripleIDs.setSubject(new LongWritable(subject));
	tripleIDs.setPredicate(new LongWritable(predicate));
	tripleIDs.setObject(new LongWritable(object));
	return tripleIDs;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.rdfhdt.mrbuilder.triples.TriplesMapper#value(org.rdfhdt.hdt.triples.TripleString)
     */
    @Override
    protected NullWritable value(TripleString tripleString) {
	return NullWritable.get();
    }

}
