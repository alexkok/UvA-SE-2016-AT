package tests;

public class DuplicationBlock_Column {
	
	
	public void f ()
	{
		int z=0; int a=1; 
		int b=2; 
		int c=3; int w=4;
	}
	
	public void h ()
	{
		int z=0; 
		int a=1;
		int b=2; 
		int c=3; 
		int w=4;
	}
	
	public void a ()
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
	
	public void b ()
	{
		if (true) { int x=0; int a=1;
			int b=2;
			int c=3;
			int w=4;
		}
		if (true) { return;
		}
	}

}
