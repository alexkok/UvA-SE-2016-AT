module Basic::Even

/**
 * Alex Kok (11353155)
 * alex.kok@student.uva.nl
 * 
 */
 
list[int] even0(int max) {
	list[int] result = [];
	for (int i <- [0..max])
		if (i % 2 == 0) 
			result += i;
	return result;
}

// Removing temporary type declaration
list[int] even1(int max) {
	result = [];
	for (int i <- [0..max])
		if (i % 2 == 0) 
			result += i;
	return result;
}


// Inlining the condition inside the for-loop
list[int] even2(int max) {
	result = [];
	for (int i <- [0..max], i % 2 == 0) 
		result += i;
	return result;
}

// "In fact, for loops may produce lists as values, using the append statement"
list[int] even3(int max) {
	result = for (int i <- [0..max], i % 2 == 0) 
		append i;
	return result;
}

// Now we can immediately return it...
list[int] even4(int max) {
	return for (int i <- [0..max], i % 2 == 0) 
		append i;
}

// But, it is close to list comprehension now:
list[int] even5(int max) {
	return [ i | i <- [0..max], i % 2 == 0];
}

// So we can just define it as a variable now
list[int] even6(int max) = [ i | i <- [0..max], i % 2 == 0];

// We can also use it as a set
set[int] even7(int max) = { i | i <- [0..max], i % 2 == 0};