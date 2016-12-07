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

public loc fileLoc = |project://MetricsTests2/src/tests/DuplicationSimple.java|;

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
	
	visit(d) {
		case node subTree: {
			//println(subTree);
			int subTreeSize = getSizeForSubTree(subTree); 
			if (subTreeSize > 10) {
				println("<subTreeSize>");
				int theIndex = getBucketIndexOfSubTree(subTreeSize, bucketSize);
                bucketList[theIndex] ? emptyNodeList += subTree;
			}
		}
	}
	println("TotalNodes: <totalNodes>");
	println("BucketSize: <bucketSize>");
	println("BucketListSize: <size(bucketList)>");
	findDuplicates(bucketList);
	println(bucketList[3][0]);
	println();
	println(bucketList[3][1]);
	//println(bucketList[2]);
}

public void findDuplicates(map[int, list[node]] bucketList) {
	clones = {};
	for (b <- bucketList) {
		println("<b> : <size(bucketList[b])>");
		treesToCheck = bucketList[b];
		for (<dup1, dup2> <- [<dup, other> | dup <- treesToCheck, other <- treesToCheck - dup, dup == other]) {
			println("Found a duplicate!");
			println("<dup1@src>");
			println("<dup2@src>");
			// The add/removing progress...
			for (sub1 <- treesToCheck) {
				;
			}
		}
	}
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