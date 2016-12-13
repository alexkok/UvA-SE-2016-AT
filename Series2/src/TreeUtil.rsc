module TreeUtil

import lang::java::jdt::m3::AST;

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

public int getNodesForTree(node d) {
 	int nodes = 0;
 	visit (d) {
 		case node n:
 			nodes += 1;
 	}
 	return nodes;
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
	for (Declaration d <- asts) {
		counter += 1;
		print("\r- Progress: <counter>/<size(asts)>");
		bottom-up visit(d) {
			case list[node] sts: { 
				int theSize = size(sts);
				if (theSize >= tresholdMinSequenceLength) {
					for (list[int] seq <- createSequencePermutations([1..theSize])) {
						list[int] indexes = [];
						list[node] statements = [];
						for (i <- seq) {
							indexes += getBucketIndexOfSubTree(getNodesForTree(sts[i]), bucketSize);
							statements += sts[i];
						}
						hash = createCustomSequenceHash(indexes);
	                	stsSize = size(statements); 
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
				}
				if (maxSeqLength < theSize) {
					maxSeqLength = theSize;
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

	for (subSeqLength <- [tresholdMinSequenceLength..maxSeqLength+1]) { // [1..5] gives me [1,2,3,4]. That's why +1
		print("\r- Progress: <subSeqLength-tresholdMinSequenceLength>/<maxSeqLength-tresholdMinSequenceLength>");
		hashMapEntriesToCheck  = subSequenceList[subSeqLength];
		
		for (hash <- hashMapEntriesToCheck) { // Order doesn't matter here: Possible clones have the same hash already
			statementListsToCheck = hashMapEntriesToCheck[hash];
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
				int fullLocLength = src1last.offset-src1first.offset + src1last.length;
				loc fullLoc1 = |project://<src1first.authority><src1first.path>|(src1first.offset, fullLocLength, <src1first.begin.line, src1first.begin.column>, <src1last.end.line, src1last.end.column>);
				loc fullLoc2 = |project://<src2first.authority><src2first.path>|(src2first.offset, fullLocLength, <src2first.begin.line, src2first.begin.column>, <src2last.end.line, src2last.end.column>);
				//println("Found duplicate sequence!");
				//println("<fullLoc1>  |  <fullLoc2>");
				
				possibleCloneToAdd1 = <[ls@src | ls <- dup1],fullLoc1>;
				possibleCloneToAdd2 = <[ls@src | ls <- dup2],fullLoc2>;
				hasBeenAdded1 = false;
				hasBeenAdded2 = false;				
				
				// Remove the subtrees because this block already has been found. As described in the paper
				visit (dup1) {
					case Statement n:
						for (<locations, fullLoc> <- clones) {
							if (size(locations) < size(possibleCloneToAdd1[0])) { 
								for (i <- [0..size(possibleCloneToAdd1[0])]) { 
									if (locations[0] == possibleCloneToAdd1[0][i]) { // TRESHOLD_SIMILARITY
										clones -= <locations, fullLoc>;
									}
								}
							}
						}
				}
				visit (dup2) {
					case Statement n:
						for (<locations, fullLoc> <- clones) {
							if (size(locations) < size(possibleCloneToAdd2[0])) { 
								for (i <- [0..size(possibleCloneToAdd2[0])]) { 
									if (locations[0] == possibleCloneToAdd2[0][i]) { // TRESHOLD_SIMILARITY
										clones -= <locations, fullLoc>;
									}
								}
							}
						}
				}
				
				// Add this clone pair to our clones
				clones += possibleCloneToAdd1;
				clones += possibleCloneToAdd2;
		   }
	   }
	}
	println();
	return clones;
}

// Helper methods
// Input: [1,2,3,4,5]
// Output: [[1,2], [1,2,3], [1,2,3,4], [1,2,3,4,5], 
// 		 	[2,3], [2,3,4], [2,3,4,5],
//			[3,4], [3,4,5],
//			[4,5]
//		   ]
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

public int getBucketIndexOfSubTree(int treeSize, int bucketSize) {
	return treeSize % bucketSize;
}

public str createCustomSequenceHash(list[int] indexes) {
	str customHash = "";
	for (i <- indexes) {
		customHash += "<i>_";
	}
	return customHash;
}