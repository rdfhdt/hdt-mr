package org.rdfhdt.hdt.dictionary.impl.section;

import java.io.IOException;
import java.io.InputStream;

import org.rdfhdt.hdt.dictionary.DictionarySectionPrivate;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.HDTSpecification;

public class DictionarySectionFactory2 extends DictionarySectionFactory {

	
	public static DictionarySectionPrivate loadFrom(InputStream input, ProgressListener listener) throws IOException {
		if(!input.markSupported()) {
			throw new IllegalArgumentException("Need support for mark()/reset(). Please wrap the InputStream with a BufferedInputStream");
		}
		input.mark(64);
		int dictType = input.read();
		input.reset();
		input.mark(64);		// To allow children to reset() and try another instance.
		
		DictionarySectionPrivate section=null;
		
		switch(dictType) {
		case PFCDictionarySection.TYPE_INDEX:
			try{
				// First try load using the standard PFC 
				section = new PFCDictionarySection(new HDTSpecification());
				section.load(input, listener);
			} catch (IllegalArgumentException e) {
				// The PFC Could not load the file because it is too big, use PFCBig
				section = new TransientDictionarySection(new HDTSpecification());
				section.load(input, listener);
			}
			return section;
		}
		throw new IOException("DictionarySection implementation not available for id "+dictType);
	}
}
