// Should be detected as 3 clone blocks
package tests;

public class DuplicationSimple_Nested {
	public void f ()
	{
		int x=0;
		int a=1;
		int b=2;
		int c=3;
		int w=4;
	}
	
	public void g ()
	{
		if (true) {
			int x=0;
			int a=1;
			int b=2;
			int c=3;
			int w=4;
		}
	}
	
	public void h ()
	{
		if (true) {
			int x=0;
			int a=1;
			int b=2;
			int c=3;
			int w=4;
		}
	}
}