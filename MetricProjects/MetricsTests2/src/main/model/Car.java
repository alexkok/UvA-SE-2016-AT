package main.model;

public class Car {
	
	private String name;
	private int wheels;
	private boolean airbag = false;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getWheels() {
		return wheels;
	}
	public void setWheels(int wheels) {
		this.wheels = wheels;
	}
	public boolean getAirbag() {
		return airbag;
	}
	
	public boolean isDriving(boolean driving) {
		boolean carIsDriving = false;
		if (driving) {
			carIsDriving = true;
		}
		
		return carIsDriving;
	}
	
	public void speed() {
		int count = 1;
		
        while (count < 130) {
            System.out.println("speed is: " + count);
            count++;
        }
	}
	
	public boolean hasAirbag() {
		return (isDriving(true)) ? true : false;
	}
	
	public boolean isCar() {
		return (name != null && wheels > 0 && true || false);
	}
	
	public void driving500KM() {
		int km = 500;
		Double fuel = 70D; 
		
		for(int i = 0; i < km; i++) {
			fuel -= 0.14;
			System.out.println("Driving " + i + " km, fuel: " + fuel);
		}
		
		

		System.out.println("SOMETEST");
		System.out.println("SOMETEST");
		System.out.println("SOMETEST");
		System.out.println("SOMETEST");
		System.out.println("SOMETEST");
		System.out.println("SOMETEST");
		System.out.println("SOMETEST");
		System.out.println("SOMETEST");
		System.out.println("SOMETEST");
		System.out.println("SOMETEST");
	}
	
	public void show() {
		System.out.println("ADSDDDDD0");
		System.out.println("ADSDDDDD1");
		System.out.println("ADSDDDDD2");
		System.out.println("ADSDDDDD3");
		System.out.println("ADSDDDDD4");
		System.out.println("ADSDDDDD5");
		System.out.println("ADSDDDDD6");
		System.out.println("ADSDDDDD7");
		System.out.println("ADSDDDDD8");
		
		System.out.println( "      ____");
		System.out.println( "_____/    |");
		System.out.println("\\_________|");
		System.out.println( "(_)     (_)");
	}
	
}
