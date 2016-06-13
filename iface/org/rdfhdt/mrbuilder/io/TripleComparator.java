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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author chemi
 *
 */
@SuppressWarnings("rawtypes")
public abstract class TripleComparator<TW extends TripleWritable> extends WritableComparator {

    public TripleComparator(Class<? extends TripleWritable> keyClass, boolean createInstances) {
	super(keyClass, createInstances);
    }

    public TripleComparator(Class<? extends TripleWritable> keyClass) {
	super(keyClass);
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
	TW key1 = (TW) wc1;
	TW key2 = (TW) wc2;
	return key1.compareTo(key2);
    }
}
