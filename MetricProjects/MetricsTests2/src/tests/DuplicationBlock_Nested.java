// Should detect 3 duplicate blocks
// Total: 17 LOC duplication
package tests;

public class DuplicationBlock_Nested {
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
		if (true) {
			return;
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
		if (true) {
			return;
		}
	}
}