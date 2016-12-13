module \test::Test_Duplication_Sequence

import IO;

import Cache;
import \test::TestHelper;

test bool testSequenceStart() {
	loc location = |project://MetricsTests2/src/tests/DuplicationSequence_Start.java|;
	
	clones = getClonesForLocation(location);
	<amountClones, amountLOC> = getSizeAndLocFromClones(clones);
	
	return (amountClones == 2 && amountLOC == 6);
}

test bool testSequenceMiddle() {
	loc location = |project://MetricsTests2/src/tests/DuplicationSequence_Middle.java|;
	
	clones = getClonesForLocation(location);
	<amountClones, amountLOC> = getSizeAndLocFromClones(clones);
	
	return (amountClones == 2 && amountLOC == 6);
}

test bool testSequenceEnd() {
	loc location = |project://MetricsTests2/src/tests/DuplicationSequence_End.java|;
	
	clones = getClonesForLocation(location);
	<amountClones, amountLOC> = getSizeAndLocFromClones(clones);
	
	return (amountClones == 2 && amountLOC == 6);
}

test bool testSequenceStartAndEnd() {
	loc location = |project://MetricsTests2/src/tests/DuplicationSequence_StartAndEnd.java|;
	
	clones = getClonesForLocation(location);
	<amountClones, amountLOC> = getSizeAndLocFromClones(clones);
	
	return (amountClones == 4 && amountLOC == 12);
}
