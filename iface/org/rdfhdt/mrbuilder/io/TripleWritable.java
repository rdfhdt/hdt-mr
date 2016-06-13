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
package org.rdfhdt.mrbuilder.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author chemi
 *
 */

@SuppressWarnings("rawtypes")
public abstract class TripleWritable<S extends WritableComparable, P extends WritableComparable, O extends WritableComparable> implements WritableComparable<TripleWritable<S, P, O>> {

    protected S subject;
    protected P predicate;
    protected O object;

    /**
     *
     */
    public TripleWritable(S subject, P predicate, O object) {
	this.setSubject(subject);
	this.setPredicate(predicate);
	this.setObject(object);
    }

    /**
     * @return the subject
     */
    public S getSubject() {
	return this.subject;
    }

    /**
     * @param subject
     *            the subject to set
     */
    public void setSubject(S subject) {
	this.subject = subject;
    }

    /**
     * @return the predicate
     */
    public P getPredicate() {
	return this.predicate;
    }

    /**
     * @param predicate
     *            the predicate to set
     */
    public void setPredicate(P predicate) {
	this.predicate = predicate;
    }

    /**
     * @return the object
     */
    public O getObject() {
	return this.object;
    }

    /**
     * @param object
     *            the object to set
     */
    public void setObject(O object) {
	this.object = object;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput input) throws IOException {
	this.subject.readFields(input);
	this.predicate.readFields(input);
	this.object.readFields(input);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput output) throws IOException {
	this.subject.write(output);
	this.predicate.write(output);
	this.object.write(output);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(TripleWritable<S, P, O> otherKey) {
	int comparison;
	if ((comparison = this.compareSubjectTo(otherKey)) == 0)
	    if ((comparison = this.comparePredicateTo(otherKey)) == 0)
		comparison = this.compareObjectTo(otherKey);
	return comparison;
    }

    public int compareSubjectTo(TripleWritable<S, P, O> otherKey) {
	return this.compareRole(this.getSubject(), otherKey.getSubject());
    }

    public int comparePredicateTo(TripleWritable<S, P, O> otherKey) {
	return this.compareRole(this.getPredicate(), otherKey.getPredicate());
    }

    public int compareObjectTo(TripleWritable<S, P, O> otherKey) {
	return this.compareRole(this.getObject(), otherKey.getObject());
    }

    @SuppressWarnings("unchecked")
    protected int compareRole(WritableComparable wc1, WritableComparable wc2) {
	return (wc1.compareTo(wc2) < 0) ? -1 : ((wc1.compareTo(wc2) > 0) ? 1 : 0);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
	return this.subject + " " + this.predicate + " " + this.object;
    }

}
