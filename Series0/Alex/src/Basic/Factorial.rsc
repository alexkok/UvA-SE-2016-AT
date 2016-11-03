module Basic::Factorial

/**
 * Alex Kok (11353155)
 * alex.kok@student.uva.nl
 * 
 */
 
public int fac(int n) = n <= 0 ? 1 : n * fac(n-1);

public int fac2(0) = 1;
public default int fac2(int n) = n * fac2(n - 1); // Why default?

public int fac3(int n) {
	if (n == 0)
		return 1;
	return n * fac3(n - 1);
}
