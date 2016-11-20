module MetricsUtil

import ParseTree;
import lang::java::\syntax::Java15;
import util::LOC;
import IO;

public int calculateLOC(type[&T<:Tree] theType, loc location) {
	print(".");
	try {
		t = parse(theType, location);
		return countSLOC(t);
	} catch ParseError(loc l): {
		println("\nFound a parse error in <l> at line <l.begin.line>, column <l.begin.column>");
		return -1;
	}
}