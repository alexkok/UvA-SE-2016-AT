module Main

/**
 * Alex Kok (11353155)
 * alex.kok@student.uva.nl
 * 
 * Thanusijan Tharumarajah (_STNUMMER_)
 * thanus.tharumarajah@student.uva.nl
 */

import IO;

public void main() {
	println("Starting metrics analysis");
	
}

/**
 * Calculating the value of the volume.
 * 
 * By calculating the LOC of the programme and check these according to the ranking scheme that SIG uses.
 * Note that since we talk about Java projects, we can compare the values of man years based on their Java specifications. 
 * See the ranking scheme below.
 *
 ****************************************
 * // TODO: THE TABLE					*
 ****************************************
 * 
 * The metrics we use to evaluate the LOC
 * - Not comments or blank lines (SIG)
 * Some samples:
 * > /* ^/ + // aaa - Will be considered as a LOC >> (where ^ will be a *)
 * > When a { or } is found as their own on a line, it will still be considered as a LOC. 
 */
public void calculateVolume() {
	int i = 1 
		/* */ + // aaa
		1 ;
}