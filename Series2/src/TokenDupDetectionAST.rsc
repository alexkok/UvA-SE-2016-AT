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

public loc fileLoc = |project://MetricsTests2/src/tests/DuplicationBlock_Nested.java|;

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
	
	list[node] emptyNodeList = [];
	
	bottom-up visit(d) {
		case node subTree: {
			//println(subTree);
			int subTreeSize = getSizeForSubTree(subTree); 
			if (subTreeSize > 10) {
				println("<subTreeSize>");
				getNameForSubTree(subTree);
				int theIndex = getBucketIndexOfSubTree(subTreeSize, bucketSize);
                bucketList[theIndex] ? emptyNodeList += subTree;
			}
		}
	}
	println("TotalNodes: <totalNodes>");
	println("BucketSize: <bucketSize>");
	println("BucketListSize: <size(bucketList)>");
	println("** Finding duplicated blocks **");
	findDuplicates(bucketList);
	//println(bucketList[3][0]);
	//println();
	//println(bucketList[3][1]);
	//println(bucketList[2]);
	println("** Finding duplicated sequences **");
	findDuplicateSequences(bucketList);
	
}

public void findDuplicateSequences(map[int, list[node]] bucketList) {

}

// Input: [1,2,3,4,5]
// Output: [[1,2], [1,2,3], [1,2,3,4], [1,2,3,4,5], 
// 		 	[2,3], [2,3,4], [2,3,4,5],
//			[3,4], [3,4,5],
//			[4,5]
//		   ]
public list[list[int]] createSequencePermutations(list[int] input) {
	list[list[int]] perms = [];
	for (i <- [1..size(input)]) {
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
 		case e:Expression\type:
 			try {
 				//println("<e>\n");
 				//name += 1;
 				;
			} catch NoSuchAnnotation (e) : {
 				println("<e>\n");
			}
 	}
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