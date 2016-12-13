module \test::Test_Cache

import Prelude;
import Cache;
import lang::java::jdt::m3::AST;

private loc cacheLoc = |project://CloneDetection/src/cache/|;

test bool testCacheAstProject() {
	setUp();
	
	loc location = |project://MetricsTests2/src/|;
	
	set[Declaration] ast = createAstsFromProject(location, false);
	set[Declaration] cachedAst = createAstsFromProject(location, true);
	
	return ast == cachedAst;
}

test bool testCacheAstFile() {
	setUp();
	
	loc location = |project://MetricsTests2/src/tests/DuplicationBlock_Simple.java|;
	
	Declaration ast = createAstFromFileC(location, false);
	Declaration cachedAst = createAstFromFileC(location, true);
	
	return ast == cachedAst;
}

test bool testCachedAstProjectIsCreated() {
	setUp();
	
	loc location = |project://MetricsTests2/src/|;
	createAstsFromProject(location, false);
	
	return exists(astCacheFile(location));
}

test bool testCachedAstFileIsCreated() {
	setUp();
	
	loc location = |project://MetricsTests2/src/tests/DuplicationBlock_Simple.java|;
	createAstFromFileC(location, false);
	
	return exists(astCacheFile(location));
}

public void setUp() {
	remove(cacheLoc);
}