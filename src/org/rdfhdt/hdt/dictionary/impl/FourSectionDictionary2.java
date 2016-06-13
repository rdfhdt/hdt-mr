package org.rdfhdt.hdt.dictionary.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.rdfhdt.hdt.dictionary.DictionarySectionPrivate;
import org.rdfhdt.hdt.dictionary.impl.section.DictionarySectionCacheAll;
import org.rdfhdt.hdt.dictionary.impl.section.DictionarySectionFactory2;
import org.rdfhdt.hdt.exceptions.IllegalFormatException;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.ControlInfo;
import org.rdfhdt.hdt.options.ControlInformation;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.util.io.CountInputStream;
import org.rdfhdt.hdt.util.listener.IntermediateListener;

public class FourSectionDictionary2 extends FourSectionDictionary {

	public FourSectionDictionary2(HDTOptions spec, DictionarySectionPrivate s, DictionarySectionPrivate p, DictionarySectionPrivate o, DictionarySectionPrivate sh) {
		super(spec, s, p, o, sh);
	}

	public FourSectionDictionary2(HDTOptions spec) {
		super(spec);
	}

	public void load(InputStream input, ControlInfo ci, ProgressListener listener) throws IOException {
		if(ci.getType()!=ControlInfo.Type.DICTIONARY) {
			throw new IllegalFormatException("Trying to read a dictionary section, but was not dictionary.");
		}
		
		IntermediateListener iListener = new IntermediateListener(listener);

		shared = DictionarySectionFactory2.loadFrom(input, iListener);
		subjects = DictionarySectionFactory2.loadFrom(input, iListener);
		predicates = DictionarySectionFactory2.loadFrom(input, iListener);
		objects = DictionarySectionFactory2.loadFrom(input, iListener);
	}
	
	@Override
	public void mapFromFile(CountInputStream in, File f, ProgressListener listener) throws IOException {
		ControlInformation ci = new ControlInformation();
		ci.load(in);
		if(ci.getType()!=ControlInfo.Type.DICTIONARY) {
			throw new IllegalFormatException("Trying to read a dictionary section, but was not dictionary.");
		}
		
		IntermediateListener iListener = new IntermediateListener(listener);
		shared = DictionarySectionFactory2.loadFrom(in, f, iListener);
		subjects = DictionarySectionFactory2.loadFrom(in, f, iListener);
		predicates = DictionarySectionFactory2.loadFrom(in, f, iListener);
		objects = DictionarySectionFactory2.loadFrom(in, f, iListener);
		
		// Use cache only for predicates. Preload only up to 100K predicates.
		predicates = new DictionarySectionCacheAll(predicates, predicates.getNumberOfElements()<100000);
	}
	
}
