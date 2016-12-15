module TreeUtil

import lang::java::jdt::m3::AST;
import ParseTree;

import util::Math;
import List;
import Set;
import IO;

alias SubSequenceList = map[int listLength, 
							map[str hashId, 
								list[
									list[node] statements
									] statementLists
								] hashMap
							];

public int getMaxNodesFromTrees(set[Declaration] asts) {
	return (0 | max(it, getNodesForTree(ast)) | ast <- asts);
}

public int getMaxStatementListsFromTree(set[Declaration] asts) {
	int counter = 0;
	visit (asts) {
		case list[node] sts:
			counter += 1;
	}
	return counter;
}

private map[node, int] nodesCache = (); 

public int getNodesForTree(node d) {
	if (!nodesCache[d]?) {
	 	int nodes = 0;
	 	visit (d) {
	 		case node n:
	 			nodes += 1;
			default:
				nodes += 1;
	 	}
	 	nodesCache += (d: nodes);
	} 
	return nodesCache[d];
}

/**
 * @Method findSubSequences
 * @Param asts: The ASTs over which the sequences have to be calculated.
 * @Param bucketSize: The bucket size of the buckets that we use to hash the ASTs.
 * @Param tresHoldMinSequenceLength: The minimum sequence lenght that should be detected. This value should be 2 or higher. 
 *
 * @Description:
 * TODO
 *
 * @Returns <subSequenceList, maxSequenceLength>: A list of the actual subsequences is being returned and the highest length of a sequence in it.  
 */
public tuple[SubSequenceList subSequenceList, int maxSequenceLength] findSubSequences(set[Declaration] asts, int bucketSize, int tresholdMinSequenceLength) {
	SubSequenceList subSequenceList = ();
	int maxSeqLength = 0;
	
	int counter = 0;
	int theSize = getMaxStatementListsFromTree(asts);
	bottom-up visit(asts) {
		case list[node] sts: {
			counter += 1;
			print("\r- Progress: [<counter>/<theSize>]");
			int theSize = size(sts);
			if (theSize >= tresholdMinSequenceLength) {
				for (list[int] seq <- createSequencePermutations([1..theSize])) {
					list[int] indexes = ([] | it + getBucketIndexOfSubTree(getNodesForTree(sts[i]), bucketSize) | i <- seq);
					list[node] statements = ([] | it + sts[i] | i <- seq);
					hash = createCustomSequenceHash(indexes);
                	stsSize = (0 | it + getNodesForTree(s) | s <- sts); 
                	if (stsSize > 10) { 
	                	if (subSequenceList[stsSize]?) { 
	                		if (subSequenceList[stsSize][hash]?) {
	                			subSequenceList[stsSize][hash] += [statements];
	            			} else {
	            				subSequenceList[stsSize] += (hash : [statements]);
	        				}
	            		} else {
	            			subSequenceList += (stsSize : (hash : [statements]));
	            		} 
            		}
					if (maxSeqLength < stsSize) {
						maxSeqLength = stsSize;
					}
				}
			}
		}
	}
	println();
	return <subSequenceList, maxSeqLength>;
}

/**
 * @Method findDuplicateSequences
 * @Param subSequenceList: The subsequence list that has been found by traversing the whole AST.
 * @Param maxSeqLength: The maximum sequence length that is in the list. 
 * @Param tresHoldMinSequenceLength: The minimum sequence lenght that should be detected. This value should be 2 or higher. 
 *
 * @Description:
 * Needing the maxSeqLength because we need to access from the starting index till the end index
 *
 * @Returns clones: A set of tuples, containing a list of locations of the statements and a location of the full clone. 
 */
public set[tuple[list[loc statementLocation] statementLocations, loc fullLocation] clone] findDuplicateSequences(subSequenceList, int maxSeqLength, int tresholdMinSequenceLength) {
	set[tuple[list[loc], loc]] clones = {};

	for (subSeqLength <- [0..maxSeqLength]) { // [1..5] gives me [1,2,3,4]. That's why +1
		print("\r- Progress: [<subSeqLength>/<maxSeqLength-1>]");
		if (subSequenceList[subSeqLength]?) {
			hashMapEntriesToCheck  = subSequenceList[subSeqLength];
			for (hash <- hashMapEntriesToCheck) { // Order doesn't matter here: Possible clones have the same hash already
				statementListsToCheck = sort(hashMapEntriesToCheck[hash]);
				// [[Statement, statement], [statement, statement]] | Comparing each [Statement] with the others 
				for (<dup1, dup2> <- [<statementListsToCheck[i], statementListsToCheck[j]> | i <- [0..size(statementListsToCheck)]
																		 , j <- [i+1..size(statementListsToCheck)]
																		 , statementListsToCheck[i] == statementListsToCheck[j] // TRESHOLD_SIMILARITY
																		 ]) {
					loc src1first = dup1[0]@src;
					loc src1last = last(dup1)@src;
					loc src2first = dup2[0]@src;
					loc src2last = last(dup2)@src;
					
					// Editing a src works too, but this  modifies the normal source. So we create a new location
					int fullLoc1Length = src1last.offset-src1first.offset + src1last.length;
					int fullLoc2Length = src2last.offset-src2first.offset + src2last.length;
					loc fullLoc1 = |project://<src1first.authority><src1first.path>|(src1first.offset, fullLoc1Length, <src1first.begin.line, src1first.begin.column>, <src1last.end.line, src1last.end.column>);
					loc fullLoc2 = |project://<src2first.authority><src2first.path>|(src2first.offset, fullLoc2Length, <src2first.begin.line, src2first.begin.column>, <src2last.end.line, src2last.end.column>);
					//println("Found duplicate sequence! C:<size(clones)> | <fullLoc1>\t| <fullLoc2>");
					//println("<fullLoc1>\t|  <fullLoc2>");
					
					possibleCloneToAdd1 = <[ls@src | ls <- dup1],fullLoc1>;
					possibleCloneToAdd2 = <[ls@src | ls <- dup2],fullLoc2>;
					
					for (<locations, fullLoc> <- clones) {
						//dup1
						if (locations[0].file == possibleCloneToAdd1[1].file  && locations[0].begin.line >= possibleCloneToAdd1[1].begin.line && last(locations).end.line <= possibleCloneToAdd1[1].end.line) {
							clones -= <locations, fullLoc>;
							println("- 1 Removed block <fullLoc>");
							println("within <possibleCloneToAdd1[1]>");
						}
									
						//dup2
						if (locations[0].file == possibleCloneToAdd2[1].file && locations[0].begin.line >= possibleCloneToAdd2[1].begin.line && last(locations).end.line <= possibleCloneToAdd2[1].end.line) {
							clones -= <locations, fullLoc>;
							println("- 2 Removed block <fullLoc>");
							println("within <possibleCloneToAdd2[1]>");
						}
					}
					
					// Add this clone pair to our clones
					clones += possibleCloneToAdd1;
					clones += possibleCloneToAdd2;
			   }
		   }
	   }
	}
	println();
	return clones;
}

public list[list[value]] createSequencePermutations(list[value] input) { // Value should be the type you give it. Have to lookup how the <:T was exactly
	list[list[value]] perms = [];
	for (i <- [0..size(input)]) {
		tmpPerm = [i];
		for (j <- [i..size(input)]) {
			tmpPerm += j+1;
			perms += [tmpPerm];
		}
	}
	return perms;
}

private map[int, int] subtreeIndexesCache = (); 

public int getBucketIndexOfSubTree(int treeSize, int bucketSize) {
	if (!subtreeIndexesCache[treeSize]?) {
		subtreeIndexesCache += (treeSize: treeSize % bucketSize);
	}
	return subtreeIndexesCache[treeSize];
}

private map[list[int], str] sequenceHashCache = (); 

public str createCustomSequenceHash(list[int] indexes) {
	if (!sequenceHashCache[indexes]?) {
		sequenceHashCache += (indexes: ("" | it + "<i>_" | i <- indexes));
	}
	return sequenceHashCache[indexes];
}