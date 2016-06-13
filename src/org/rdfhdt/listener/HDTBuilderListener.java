package org.rdfhdt.listener;

import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.mrbuilder.HDTBuilderConfiguration;

public class HDTBuilderListener implements ProgressListener {

	boolean	quiet;

	public HDTBuilderListener(HDTBuilderConfiguration conf) {
		this.quiet = conf.getQuiet();
	}

	public HDTBuilderListener(boolean quiet) {
		this.quiet = quiet;
	}

	@Override
	public void notifyProgress(float level, String message) {
		if (!this.quiet) {
			System.out.print("\r" + message + "\t" + Float.toString(level) + "                            \r");
		}
	}
}