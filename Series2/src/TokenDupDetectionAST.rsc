module TokenDupDetectionAST

import ParseTree;
import vis::ParseTree;
import lang::java::\syntax::Java15;
import IO;
import String;
import List;
import Set;
import Map;
import lang::java::m3::Core;
import lang::java::jdt::m3::Core;
import lang::java::jdt::m3::AST;

private int TRESHOLD_MIN_SUBTREE_LENGTH = 10; // The minimum length (amount of nodes connected) of a sub-tree in order to detect it as a duplicate. (Described as MassTreshold in the paper)
private int TRESHOLD_MIN_SEQUENCE_LENGTH = 2; // The minimum sequence length of a block in order to detect duplicates in it

public loc fileLoc = |project://MetricsTests2/src/tests/DuplicationSequence_Middle.java|;

public void parseSomeTree() {
	projectM3Model = createM3FromEclipseProject(fileLoc);
	//set[Declaration] a = createAstsFromFile(files(projectM3Model), true);
	//for (d <- a) {
	//	iprintln(d);
	//	println();
	//}
	Declaration d = createAstFromFile(fileLoc, true);
	
	int totalNodes = getSizeForSubTree(d);
	int bucketSize = (totalNodes > 9) ? totalNodes / 10 : totalNodes;
	
	map[int, list[node]] bucketList = ();
	map[str, list[list[node]]] sequenceList = ();
	
	list[node] emptyNodeList = [];
	
	int maxSeqLength = 0;
	
	bottom-up visit(d) {
		case node subTree: {
			//println(subTree);
			int subTreeSize = getSizeForSubTree(subTree); 
			if (subTreeSize >= TRESHOLD_MIN_SUBTREE_LENGTH) {
				println("<subTreeSize>");
				getNameForSubTree(subTree);
				int theIndex = getBucketIndexOfSubTree(subTreeSize, bucketSize);
                bucketList[theIndex] ? emptyNodeList += subTree;
			}
		}
		case list[node] sts: {
			int theSize = size(sts);
			if (theSize >= TRESHOLD_MIN_SEQUENCE_LENGTH) {
				println("Found a statement list! Size: <theSize>");
				//println(sts);
				for (list[int] seq <- createSequencePermutations([1..theSize])) {
					//int theIndex = getBucketIndexOfSubTree(sts[seq, bucketSize);
					list[int] indexes = [];
					list[node] statements = [];
					list[str] names = [];
					for (i <- seq) {
						//println("SubtreeSize: <getSizeForSubTree(sts[i])> | BucketSize: <bucketSize>");
						indexes += getBucketIndexOfSubTree(getSizeForSubTree(sts[i]), bucketSize);
						statements += sts[i];
						getNameForSubTree(sts[i]);
					}
					hash = createCustomSequenceHash(indexes);
					//println("Adding sequence to list. Seq: <seq> | Indexes: <indexes> | Hash: <hash> | ");
                	sequenceList[hash] ? [emptyNodeList] += [statements];
     ;
				}
			}
			if (maxSeqLength < theSize) {
				maxSeqLength = theSize;
			}
		}
	}
	println("TotalNodes: <totalNodes>");
	println("BucketSize: <bucketSize>");
	println("BucketListSize: <size(bucketList)>");
	println("SequenceListSize: <size(sequenceList)>");
	println("** Finding duplicated blocks **");
	findDuplicates(bucketList);
	//println(bucketList);
	//println();
	//println(bucketList[3][1]);
	//println(bucketList[2]);
	println("** Finding duplicated sequences **");
	findDuplicateSequences(sequenceList);
	//println(sequenceList);
}

public str createCustomSequenceHash(list[int] indexes) {
	str customHash = "";
	for (i <- indexes) {
		customHash += "<i>_";
	}
	return customHash;
}

public void findDuplicateSequences(map[str, list[list[node]]] sequenceList) {
	// Note that the first node of each list is just the empty list :( 
	
	
	
	
	
	//iprintln(sequenceList["0_0_"][0]);
	//iprintln(sequenceList["0_0_"][1]);
	for (key <- sequenceList) {
		println(key);
		sequencesToCheck = sequenceList[key];
		//println(sequencesToCheck[1][0]);
		//println(sequencesToCheck[1][1]);
		
		for (<dup1, dup2> <- [<sequencesToCheck[i], sequencesToCheck[j]> | i <- [1..size(sequencesToCheck)], // 1 to fix the empty list problem 
																		   j <- [i+1..size(sequencesToCheck)], 
																		   sequencesToCheck[i] == sequencesToCheck[j]]) {
			println("Found duplicate sequence!");
			println(dup1[0]@src);
			println(dup1[1]@src);
			if (size(dup1) > 2) {
				println(dup1[2]@src);
			}
			println();
			println(dup2[0]@src);
			println(dup2[1]@src);
			if (size(dup2) > 2) {
				println(dup2[2]@src);
			}
			;
		}
		
	}
}

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

public void findDuplicates(map[int, list[node]] bucketList) {
	set[tuple[node, loc]] clones = {};
	for (key <- bucketList) {
		println("<key> : <size(bucketList[key])>");
		list[node] treesToCheck = bucketList[key];
		
		for (<dup1, dup2> <- [<treesToCheck[i], treesToCheck[j]> | i <- [0..size(treesToCheck)], 
																   j <- [i+1..size(treesToCheck)], 
																   treesToCheck[i] == treesToCheck[j]]) {
			println("Found duplicate!");
			println(dup1@src);
			// Remove the subtrees because this is the partent. As described in the paper
			// For each subtree s of dup1
			// 	if IsMember(clones, s) { RemoveClonePair(s) }
			visit (dup1) {
				case Statement n: 
					clones -=  <n, n@src>;
			}
			// For each subtree s of dup2
			// 	if IsMember(clones, s) { RemoveClonePair(s) }
			visit (dup2) {
				case Statement n:
					clones -= <n, n@src>;
			}
			clones += <dup1, dup1@src>;
			clones += <dup2, dup2@src>;
			//println(dup2@src);
		}
	}
	println("Clones: <size(clones)>");
	for (<dup, srcloc> <- clones) {
		println(srcloc);
	}
}

public str getNameForSubTree(d) {
 	str name = "";
 	visit (d) {
 		case Statement e:
 			try {
 				//println("<readFile(e@\src)>\n");
 				name += ("" | it + trim(line) | line <- readFileLines(e@\src));
 				//name += 1;
 				;
			} catch NoSuchAnnotation (e) : {
 				println("<e>\n");
			}
 	}
 	println(name);
 	return name;
}

public int getSizeForSubTree(d) {
 	int nodes = 0;
 	visit (d) {
 		case node n:
 			nodes += 1;
 	}
 	return nodes;
}

public int getBucketIndexOfSubTree(int treeSize, int bucketSize) {
	return treeSize % bucketSize;
}