module Basic::BottlesOfBeer

import IO;

/**
 * Alex Kok (11353155)
 * alex.kok@student.uva.nl
 * 
 */
 
str bottles(0) 	= "no more bottles";
str bottles(1)	= "1 bottle";
default str bottles(int n) = "<n> bottles";

public void sing() {
	for (n <- [99..0]) {
		println("<bottles(n)> of beer on the wall, <bottles(n)> of beer.");
		println("Take one down, pass it around, <bottles(n-1)> of beer on the wall.\n");
	}
	println("No more bottles of beer on the wall, no more bottles of beer.");
	println("Go to the store and buy some more, 99 bottles of beer on the wall.");
}