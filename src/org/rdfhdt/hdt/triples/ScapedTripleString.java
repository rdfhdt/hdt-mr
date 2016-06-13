package org.rdfhdt.hdt.triples;

import org.rdfhdt.hdt.exceptions.ParserException;

/**
 * TripleString holds a triple as Strings
 */
public final class ScapedTripleString extends TripleString {

	public ScapedTripleString() {
		super();
	}

	public ScapedTripleString(CharSequence subject, CharSequence predicate, CharSequence object) {
		super(subject, predicate, object);
	}

	public ScapedTripleString(TripleString other) {
		super(other);
	}

	/**
	 * Read from a line, where each component is separated by space.
	 *
	 * @param line
	 */
	@Override
	public void read(String line) throws ParserException {
		int split, posa, posb;
		this.clear();

		// SET SUBJECT
		posa = 0;

		if (line.charAt(posa) == '<') { // subject between '<' and '>' symbols
			posa++; // Remove <
			posb = line.indexOf('>', posa);
			split = posb + 1;
		} else { // subject until the first space
			posb = split = line.indexOf(' ', posa);
		}
		if (posb == -1) {
			return; // Not found, error.
		}

		this.setSubject(line.substring(posa, posb));

		// SET PREDICATE
		posa = split + 1;

		if (line.charAt(posa) == '<') { // predicate between '<' and '>' symbols
			posa++; // Remove <
			posb = line.indexOf('>', posa);
			split = posb + 1;
		} else { // predicate until the first space
			posb = split = line.indexOf(' ', posa);
		}
		if (posb == -1) {
			return; // Not found, error.
		}

		this.setPredicate(line.substring(posa, posb));

		// SET OBJECT
		posa = split + 1;
		posb = line.length();

		if (line.charAt(posb - 1) == '.') {
			posb--; // Remove trailing <space> <dot> from NTRIPLES.
		}
		if (line.charAt(posb - 1) == ' ') {
			posb--;
		}

		if (line.charAt(posa) == '<') {
			posa++;

			// Remove trailing > only if < appears, so "some"^^<http://datatype> is kept as-is.
			if (posb > posa && line.charAt(posb - 1) == '>') {
				posb--;
			}
		}

		this.setObject(line.substring(posa, posb));
	}
}
