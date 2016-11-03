module Common::ColoredTrees

/**
 * Alex Kok (11353155)
 * alex.kok@student.uva.nl
 * 
 */
 
 // Definition of the ColoredTree
 data ColoredTree = leaf(int n)
 				  | red(ColoredTree left, ColoredTree right)
 				  | black(ColoredTree left, ColoredTree right);

// Count the number of red nodes
public int cntRed(ColoredTree t) {
	int c = 0;
	visit(t) {
		case red(_,_): c = c + 1;
	};
	return c;
}

// Compute the sum of all integer leafs
public int sumLeafs(ColoredTree t) {
	int c = 0;
	visit(t) {
		case leaf(int n): c = c + n;
	}
	return c;
}

// Add green nodes to the ColoredTree
data ColoredTree = green(ColoredTree left, ColoredTree right);

// Transform red nodes to green nodes
public ColoredTree redToGreen(ColoredTree t) {
	return visit(t) {
		case red(l, r) => green(l, r)
	};
}

public ColoredTree aColoredTree = red(black(leaf(1), red(leaf(2),leaf(3))), black(leaf(3), leaf(4)));