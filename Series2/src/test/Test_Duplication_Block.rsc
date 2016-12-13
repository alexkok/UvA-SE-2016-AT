module \test::Test_Duplication_Block

import Set;

import Config;
import Cache;
import TreeUtil;

test bool testBlockSimple() {
	loc location = |project://MetricsTests2/src/tests/DuplicationBlock_Simple.java|;
	
	Declaration d = createAstFromFileC(location, false);
	subSequences = findSubSequences({d}, bucketSize, TRESHOLD_MIN_SEQUENCE_LENGTH);
	clones = findDuplicateSequences(subSequenceList, maxSequenceLength, TRESHOLD_MIN_SEQUENCE_LENGTH);
	
	return (size(clones) == 2); // && LOC = 10...
}

