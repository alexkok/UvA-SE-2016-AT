module \test::TestHelper

import List;
import Set;
import lang::java::jdt::m3::AST;
import IO;

import Cache;
import Config;
import TreeUtil;

bool isDebug = true;

public set[tuple[list[loc statementLocation] statementLocations, loc fullLocation] clone] getClonesForLocation(loc location) {
	Declaration d = createAstFromFileC(location, false);
	int maximumNodes = getMaxNodesFromTrees({d});
	int bucketSize = (maximumNodes > 9) ? maximumNodes / 10 : maximumNodes; // 10 % of the maximum nodes
	
	subSequences = findSubSequences({d}, bucketSize, TRESHOLD_MIN_SEQUENCE_LENGTH);
	subSequenceList = subSequences.subSequenceList;
	maxSequenceLength = subSequences.maxSequenceLength;
	
	return findDuplicateSequences(subSequenceList, maxSequenceLength, TRESHOLD_MIN_SEQUENCE_LENGTH);
}

public tuple[int amountClones, int amountLOC] getSizeAndLocFromClones(set[tuple[list[loc statementLocation] statementLocations, loc fullLocation] clone]  clones) {
	//for (<clns, fullLoc> <- clones) {
	//	println();
	//	println("- First: <clns[0]>");
	//	println("- Full:  <fullLoc>");
	//}

	int amountClones = size(clones);
	int amountLOC = (0 | it + size(readFileLines(e)) | <_,e> <- clones);
	
	if (isDebug) println("- Found <amountClones> clones of <amountLOC> LOC");
	return <amountClones, amountLOC>;
}