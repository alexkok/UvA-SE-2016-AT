module Cache

import Prelude;
import lang::java::m3::Core;
import lang::java::jdt::m3::Core;
import lang::java::jdt::m3::AST;

private loc cacheLoc = |project://CloneDetection/src/cache/|;

public set[Declaration] createAstsFromProject(loc project, bool useCache, bool debug = false) {
	set[Declaration] ast;
	
	if (!exists(astCacheFile(project)) || !useCache) {
		writeCacheProject(project, debug);
	}
	
	return loadCacheProject(astCacheFile(project), debug);
}

public Declaration createAstFromFileC(loc file, bool useCache, bool debug = false) {
	Declaration ast;
	
	if (!exists(astCacheFile(file)) || !useCache) {
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
	Declaration ast = createAstFromEclipseFile(file, true);
	
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
