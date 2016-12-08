module MainOld

import Prelude;
import lang::java::m3::Core;
import lang::java::jdt::m3::Core;
import lang::java::jdt::m3::AST;
import ParseTree;
import vis::ParseTree;
import lang::java::\syntax::Java15;

public void main() {
	loc project = |project://MetricsTests2/src|;
	projectM3Model = createM3FromEclipseProject(project);
	
	//set[Declaration] asts = createAstsFromEclipseProject(project, true);
	Declaration asts = createAstsFromEclipseFile(|project://MetricsTests2/src/main/Main.java|, true);
	int subtrees = 0;
	
	map[int, list[node]] bucketList = ();
	list[node] emptyNodeList = [];
	
	map[int, node] clones = ();
	
	int maxBucketSize = bucketSize(sizeNodes(asts));
	 
	//creating buckets with subtrees
	visit(asts) {
		case node subTree: {
			if (sizeNodes(subTree) > 5) {
				subtrees += 1;
				
				int bucketId = bucketIndex(sizeNodes(subTree), maxBucketSize);
				bucketList[bucketId] ? emptyNodeList += subTree;
			}
		}
	}
	
	//find duplicates in each bucket
	for (b <- bucketList) {
		for(st <- bucketList[b]) {
			if (st in bucketList[b] - st) {
				println(st);
				
				println("dup with:");
				
				for(bSt <- bucketList[b] - st) {
					println(bSt);
				}
				
			}
		}
	}
	
	println("\nsubtrees: <subtrees>");
	println("max bucket size: <maxBucketSize>");
	println("bucketlist <size(bucketList)>");
}

public int sizeNodes(ast) {
	int count = 0;
	
	visit(ast) {
		case node n: {
			count += 1;
		}
	}
	
	return count;
}

public int bucketSize(int nodeSize) {
	return nodeSize * 10 / 100;
}

public int bucketIndex(depth, maxBucketSize) {
	return depth % maxBucketSize;
}
