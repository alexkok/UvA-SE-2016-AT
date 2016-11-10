package main.model;

public class Car {
	
	private String name;
	private int wheels;
	
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
	
	public boolean isDriving(boolean driving) {
		boolean carIsDriving = false;
		if (driving) {
			carIsDriving = true;
		}
		
		return carIsDriving;
	}
	
}
