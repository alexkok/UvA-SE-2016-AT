module Cache

import Prelude;
import lang::java::m3::Core;
import lang::java::jdt::m3::Core;
import lang::java::jdt::m3::AST;

private loc cacheLoc = |project://CloneDetection/src/cache/|;

public set[Declaration] createAstsFromProject(loc project, bool useCache, bool debug = false) {
	set[Declaration] ast;
	
	if(useCache) {
		if(!exists(astCacheFile(project))) {
			writeCacheProject(project, debug);
		}
	} else {
		writeCacheProject(project, debug);
	}
	
	return loadCacheProject(astCacheFile(project), debug);
}

public Declaration createAstFromFileC(loc file, bool useCache, bool debug = false) {
	Declaration ast;
	
	if(useCache) {
		if(!exists(astCacheFile(file))) {
			writeCacheFile(file, debug);
		}
	} else {
		writeCacheFile(file, debug);
	}
	
	return loadCacheFile(astCacheFile(file), debug);
}

public void writeCacheProject(loc project, bool debug) {
	set[Declaration] ast = createAstsFromEclipseProject(project, true);
	
	if (debug) println("writing cache file to: <astCacheFile(project)>");
	writeBinaryValueFile(astCacheFile(project), ast);
}

public void writeCacheFile(loc file, bool debug) {
	Declaration ast = createAstFromFile(file, true);
	
	if (debug) println("writing cache file to: <astCacheFile(file)>");
	writeBinaryValueFile(astCacheFile(file), ast);
}

public set[Declaration] loadCacheProject(loc file, bool debug) {
	if (debug) println("load cache file: <file>");
	return readBinaryValueFile(#set[Declaration], file);
}

public Declaration loadCacheFile(loc file, bool debug) {
	if (debug) println("load cache file: <file>");
	return readBinaryValueFile(#Declaration, file);
}

public loc astCacheFile(loc file) {
	return cacheLoc + "<file.authority + file.path>.ast";
}
