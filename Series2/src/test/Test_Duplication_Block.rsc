module \test::Test_Duplication_Block

import IO;

import Cache;
import \test::TestHelper;

test bool testBlockSimple() {
	loc location = |project://MetricsTests2/src/tests/DuplicationBlock_Simple.java|;
	
	clones = getClonesForLocation(location);
	<amountClones, amountLOC> = getSizeAndLocFromClones(clones);
	
	return (amountClones == 2 && amountLOC == 10);
}

test bool testBlockMultiple() {
	loc location = |project://MetricsTests2/src/tests/DuplicationBlock_Multiple.java|;
	
	clones = getClonesForLocation(location);
	<amountClones, amountLOC> = getSizeAndLocFromClones(clones);
	
	return (amountClones == 4 && amountLOC == 20);
}

test bool testBlockNested() {
	loc location = |project://MetricsTests2/src/tests/DuplicationBlock_Nested.java|;
	
	clones = getClonesForLocation(location);
	<amountClones, amountLOC> = getSizeAndLocFromClones(clones);
	
	return (amountClones == 3 && amountLOC == 25);
}
