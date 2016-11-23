module metrics::Duplication

import String;
import List;
import IO;

public list[tuple[int, list[int], str]] findDuplications(str src, bool isDebug) {
	list[str] lines = [trim(l) | l <- split("\r\n", src)]; 
	
	int theSize = size(lines);
	
	list[tuple[int lnNumber, list[int] dupLs, str dupStr]] thelist = [];
	int lastC1 = 0;
	for (<c1,c2> <- [<c1,c2> | c1 <- [0..theSize], c2 <- [c1+1..theSize], lines[c1] == lines[c2]]) {
		if (isDebug && lastC1 != c1) {
			println("Checking line [<c1>/<theSize>]");
			//print(".");
			lastC1 = c1;
		}
		
		tupKey = getIndexOf(thelist, c1);
		if (tupKey != -1) { 
			// If found, merge this value in the list
			thelist[tupKey].dupLs = thelist[tupKey].dupLs + c2;
		} else {
			// -1 means "not found", add new value to the list
			thelist += <c1, [c2], lines[c1]>;
		}
	}
	
	return thelist;
}

public list[tuple[str, int]] calculateDuplicationBlocks(list[tuple[int lnNumber, list[int] dupLs, str dupStr]] theList, bool isDebug)  {
	list[tuple[str duplicateBlock, int lines]] duplicatesList = [];
	int curLnNumber = 0;
	for (i <- [0..size(theList)], theList[i].lnNumber >= curLnNumber) {
		tup = theList[i];
		//if (isDebug) println("[<i>/<size(theList)>]");
		print(".");

		int maxValue = 0;
		int j = 0;
		while (j < size(theList[i].dupLs)) {
			v = theList[i].dupLs[j];
			
			checkIndex = getIndexOf(theList, tup.lnNumber+5);
			if (v+5 in theList[checkIndex].dupLs) {
				checkIndex4 = getIndexOf(theList, tup.lnNumber+4);
				checkIndex3 = getIndexOf(theList, tup.lnNumber+3);
				checkIndex2 = getIndexOf(theList, tup.lnNumber+2);
				checkIndex1 = getIndexOf(theList, tup.lnNumber+1);
				if (v+4 in theList[checkIndex4].dupLs &&
					v+3 in theList[checkIndex3].dupLs &&
					v+2 in theList[checkIndex2].dupLs &&
					v+1 in theList[checkIndex1].dupLs ) {
					
					// * Note: We do not add \r\n here, add this if needed...
					str theDuplicate = tup.dupStr + 
						theList[checkIndex1].dupStr + 
						theList[checkIndex2].dupStr + 
						theList[checkIndex3].dupStr + 
						theList[checkIndex4].dupStr + 
						theList[checkIndex].dupStr;
						
					blockSize = 6;
					while (v+blockSize in theList[getIndexOf(theList, tup.lnNumber+blockSize)].dupLs) {
						theDuplicate += theList[getIndexOf(theList, tup.lnNumber+blockSize)].dupStr;
						blockSize += 1;
					}
					
					// The final check for real duplicates...
					if (v notin [tup.lnNumber..tup.lnNumber+blockSize-1]) {
						if (isDebug) println("\n<tup.lnNumber>:<tup.lnNumber+blockSize-1>|<v>-<v+blockSize-1>| Found block! <tup.lnNumber> | <blockSize-1> | <substring(theDuplicate, 0, (size(theDuplicate) > 150) ? 150 : size(theDuplicate))>");
						duplicatesList += <theDuplicate, blockSize-1>;
						j += blockSize-1;
						if (tup.lnNumber+blockSize-1 > maxValue) {
							maxValue = tup.lnNumber+blockSize-1;
						}
					}
				}
			}
			j += 1;
		}
		if (maxValue != 0) {
			curLnNumber = maxValue;
		}
	}
	return duplicatesList;
}

public int calculateDuplicationResult(int duplicatedLines, int volume) {
	int percentage = duplicatedLines * 100 / volume;
	if (percentage <= 3) 
		return 5;
	else if (percentage <= 5) 
		return 4;
	else if (percentage <= 10) 
		return 3;
	else if (percentage <= 20)
		return 2;
	else
		return 1;
}

private int getIndexOf(list[tuple[int lnNumber, list[int] dupLs, str dupStr]] tupleList, int lineNumber) {
	for (int i <- [0..size(tupleList)]) {
		if (lineNumber == tupleList[i].lnNumber) return i;
	}
	return -1;
}