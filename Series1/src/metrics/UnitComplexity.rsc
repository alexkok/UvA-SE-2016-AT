module metrics::UnitComplexity

import lang::java::m3::Core;
import lang::java::jdt::m3::Core;
import lang::java::jdt::m3::AST;

/**
 * Calculate Unit Complexity: For each method calculate the LOC.
 */
public list[tuple[loc, int, int]] calculateUnitComplexity(list[tuple[loc, int, int]] methodsLOC, M3 projectModel) {
	return [calculateMethodComplexity(<ml,nl,c>, projectModel) | <ml, nl, c> <- methodsLOC];
}

/** 
 * Divide the methods with their LOC in the categories that we need
 * 
 * These categories are the same as SIG uses in their paper.
 * Every method has atleast a complexity of 1.
 * The table:
 * ----------------
 * Category |  LOC
 * ----------------
 * Very high| > 50
 * High     | 21-50
 * Medium   | 11-20
 * Low      |  1-10
 */
public map[int, int] calculateUnitComplexityCategories(list[tuple[loc, int, int]] methodsLOC) {
	map[int, int] amountPerCategory = (1: 0, 2: 0, 3: 0, 4: 0);
	for (<_,n,_> <- methodsLOC) { // We are not interested in the method location here, only interested in the amount LOC.
		if (n > 0) {
			if (n <= 10) {
				amountPerCategory[1] += n;
			} else if (n <= 20) {
				amountPerCategory[2] += n;
			} else if (n <= 50) {
				amountPerCategory[3] += n;
			} else {
				// It's above 50!
				amountPerCategory[4] += n;
			}
		} 
		// Else: We had a parse error for this file, ignoring this
	}
	return amountPerCategory;
}
/**
 * Calculate Unit Complexity result: From the result, return the actual score.
 */
public int calculateUnitComplexityResult(map[int, int] categories, int totalVolume) {
	return calculateResultFromCategories(categories, totalVolume);
}


private tuple[loc, int, int] calculateMethodComplexity(tuple[loc location, int lines, int complexity] methodData, M3 projectModel) {
	int complexity = 1;
	Declaration d = getMethodASTEclipse(methodData.location, model=projectModel);
	
	int ifStatements = 0, 
		forStatements = 0, 
		whileStatements = 0, 
		caseStatements = 0, 
		catchStatements = 0,
		andStatements = 0,
		orStatements = 0,
		ternaryStatements = 0,
		assertStatements = 0
		;
	
	visit (d) {
		case \if(_,_) : 		ifStatements    += 1; // if-then 
		case \if(_,_,_) : 		ifStatements    += 1; // if-then-else
		case \foreach(_,_,_) : 	forStatements   += 1; // foreach
		case \for(_,_,_,_): 	forStatements   += 1; // for | with 4 params
		case \for(_,_): 		forStatements   += 1; // for | with 2 params
		case \while(_,_): 		whileStatements += 1; // while
		case \case(_): 			caseStatements  += 1; // case
		case \catch(_): 		catchStatments  += 1; // catch
		case infix(_,"&&",_): 	andStatements  	+= 1; // &&
		case infix(_,"||",_): 	orStatements  	+= 1; // &&
		case conditional(_,_,_): ternaryStatements+= 1; // ? | ternary operator
		case \assert(_): 		assertStatements +=1; // Assert
		case \assert(_,_): 		assertStatements +=1; // Assert
		// * Note: We do not take "return" statements into account.
	}
	
	complexity += ifStatements 
			   += forStatements
			   += whileStatements
			   += caseStatements
			   += catchStatements
			   += andStatements
			   += orStatements
			   += ternaryStatements
			   += assertStatements
			   ;
	methodData.complexity= complexity;
	return methodData;
}


/**
 * Calculate the actual result given the categories
 * This table is equal to the one that SIG uses in their paper
 * The table:        
 * --------- Maximum relative LOC ---
 * Rank | Moderate | High | Very high 
 * ----------------------------------
 *  + + |   25%    |  0%  |   0%
 *   +  |   30%    |  5%  |   0%
 *   0  |   40%    | 10%  |   0%
 *   -  |   50%    | 15%  |   5%
 *  - - |    -     |  -   |    -
 */
private int calculateResultFromCategories(map[int, int] categories, int totalVolume) {
	map[int, int] percentsPerCategory = (
		1: categories[1] * 100 / totalVolume, 
		2: categories[2] * 100 / totalVolume, 
		3: categories[3] * 100 / totalVolume, 
		4: categories[4] * 100 / totalVolume 
		);
	
	// Now we know the LOC per category, we can define the result.
	if (percentsPerCategory[2] <= 25 && percentsPerCategory[3] == 0 && percentsPerCategory[4] == 0) 
		return 5; 
	else if (percentsPerCategory[2] <= 30 && percentsPerCategory[3] <= 5 && percentsPerCategory[4] == 0) 
		return 4;
	else if (percentsPerCategory[2] <= 40 && percentsPerCategory[3] <= 10 && percentsPerCategory[4] == 0) 
		return 3;
	else if (percentsPerCategory[2] <= 50 && percentsPerCategory[3] <= 15 && percentsPerCategory[4] <= 5) 
		return 2;
	else 
		return 1;
}