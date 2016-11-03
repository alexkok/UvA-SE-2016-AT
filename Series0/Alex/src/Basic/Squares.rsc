module Basic::Squares

import IO;

/**
 * Alex Kok (11353155)
 * alex.kok@student.uva.nl
 * 
 */

public void squares(int n) {
	println("Table of squares from 1 to <n>");
	for(int i <- [1.. n+1])
		println("<i> squared = <i * i>");
}

// With multi-line string template:
public str squaresTemplate(int n) 
	= "Table of squares from 1 to <n>
	  '<for (int i <- [1..n+1]) {>
	  '	<i> squared = <i * i><}>
	  ";