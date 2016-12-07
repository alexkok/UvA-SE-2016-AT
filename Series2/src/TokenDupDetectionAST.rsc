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
				getNameForSubTree(subTree);
				int theIndex = getBucketIndexOfSubTree(subTreeSize, bucketSize);
                bucketList[theIndex] ? emptyNodeList += subTree;
			}
		}
	}
	println("TotalNodes: <totalNodes>");
	println("BucketSize: <bucketSize>");
	println("BucketListSize: <size(bucketList)>");
	findDuplicates(bucketList);
	//println(bucketList[3][0]);
	//println();
	//println(bucketList[3][1]);
	//println(bucketList[2]);
}

public void findDuplicates(map[int, list[node]] bucketList) {
	clones = {};
	for (key <- bucketList) {
		println("<key> : <size(bucketList[key])>");
		treesToCheck = bucketList[key];
		if (key == 1) {
			println(treesToCheck[0]);
			println(treesToCheck[1]);
		}
		// Make own forloop I guess..
		for (<dup1, dup2> <- [<dup, other> | dup <- treesToCheck, other <- treesToCheck - dup]) {//, dup == other]) {
			if (dup1 == dup2) {
				println("Duplicate!");
				println("- <dup1@src>");
				//println(dup1);
				println("- <dup2@src>");
				//println(dup2);
				// The add/removing progress...
				for (sub1 <- treesToCheck) {
					;
				}
			} else {
				println("No duplicate!");
				println("- <dup1@src>");
				//println(dup1);
				println("- <dup2@src>");
				//println(dup2);
			}
		}
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