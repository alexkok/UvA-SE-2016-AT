module Common::CountConstructors

import Node;
import Map;

/**
 * Alex Kok (11353155)
 * alex.kok@student.uva.nl
 * 
 */

 // Definition of the ColoredTree
 data ColoredTree = leaf(int n)
 				  | red(ColoredTree left, ColoredTree right)
 				  | black(ColoredTree left, ColoredTree right);

public ColoredTree aColoredTree = red(black(leaf(1), red(leaf(2),leaf(3))), black(leaf(3), leaf(4)));

// Definition of Cards
data Suite = hearts() | diamonds() | clubs() | spades();
data Card = ace(Suite s) | two(Suite s) | three(Suite s) | four(Suite s) | five(Suite s)  |
            six(Suite s) | seven(Suite s) | eight(Suite s) | nine(Suite s) | ten(Suite s) |
            jack(Suite s) | queen(Suite s) | king(Suite s);

data Hand = hand(list[Card] cards);

public Hand aHand = hand([two(hearts()), jack(diamonds()), six(hearts()), ace(spades())]);

// Count frequencies of constructors
public map[str, int] count (node n) {
	freq = ();
	visit(n) {
		case node m: {
			name = getName(m);
			freq[name] ? 0 += 1;
		}
	}
	return freq;
}

public map[str, int] countRelevant(node n, set[str] relevant) = domainR(count(n), relevant);

// Some testing:
// > count(aColoredTree)
// map[str, int]: ("red":2,"leaf":5,"black":2)

// >count(a)
// aColoredTree   aHand          ace
            
// >count(aaHand)
// map[str, int]: ("six":1,"ace":1,"two":1,"hearts":2,"spades":1,"hand":1,"diamonds":1,"jack":1)

// >countRelevant(aHand, {"hearts", "spades"});
// map[str, int]: ("hearts":2,"spades":1)