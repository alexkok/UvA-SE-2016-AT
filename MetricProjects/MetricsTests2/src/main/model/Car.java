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
	
	public void driving500KM() {
		int km = 500;
		Double fuel = 70D; 
		
		for(int i = 0; i < km; i++) {
			fuel -= 0.14;
			System.out.println("Driving " + i + " km, fuel: " + fuel);
		}
	}
	
}
