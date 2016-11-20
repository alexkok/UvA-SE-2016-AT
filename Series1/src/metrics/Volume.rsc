module metrics::Volume

import lang::java::m3::Core;

import MetricsUtil;

public int calculateVolume(M3 projectModel) {
	set[loc] files = files(projectModel);
	return (0 | it + n | <_, n> <- filesLOC(files), n != -1); // -1 means it was an error, so we should ignore it here.
}

/**
 * Since we only parse java files currently, we can take over the man years table from SIG for the Java language
 * The table: 
 * Rank | Man years  | KLOC Java
 * ----------------------------
 *  + + |   0 - 8    |   0 - 66
 *   +  |   8 - 30   |  66 - 246
 *   0  |  30 - 80   | 245 - 665
 *   -  |  80 - 160  | 655 - 1310
 *  - - |   > 160    |   > 1310
 */
public str calculateVolumeResult(int linesOfCode) {
	if (linesOfCode < 66000) {
		return "++";
	} else if (linesOfCode < 246000) {
		return "+";
	} else if (linesOfCode < 665000) {
		return "0";
	} else if (linesOfCode < 1310000) {
		return "-";
	} else {
		return "--";
	}
}

private list[tuple[loc, int]] filesLOC(set[loc] files) {
	// Defining the type of the tree that the parse tree needs to parse. 
	// #start | To ignore any layout things before the first element is found (CompilationUnit in this case)
	// CompilationUnit | Because we are working with files, the files begin with the root, which is CompilationUnit in the defined grammar.
	
	//type[&T<:Tree]  
	theType = #start[CompilationUnit]; 
	
	return [<f, calculateLOC(theType, f)> | f <- files];
}