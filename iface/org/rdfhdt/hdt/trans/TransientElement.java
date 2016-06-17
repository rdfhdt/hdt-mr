/**
 *
 */
package org.rdfhdt.hdt.trans;

import java.io.IOException;

import org.apache.hadoop.io.SequenceFile;
import org.rdfhdt.hdt.listener.ProgressListener;

/**
 * @author chemi
 *
 */
public interface TransientElement {

    public void initialize(long numentries);

    public void load(SequenceFile.Reader input, ProgressListener listener) throws IOException;

    public void close() throws IOException;

}
