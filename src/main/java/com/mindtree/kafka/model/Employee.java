package com.mindtree.kafka.model;

public class Employee {
private String name;
private String unit;
private Double salary;

public Employee() {
	 
}
public String getName() {
	return name;
}
public void setName(String name) {
	this.name = name;
}
public String getUnit() {
	return unit;
}
public void setUnit(String unit) {
	this.unit = unit;
}
public Double getSalary() {
	return salary;
}
public void setSalary(Double salary) {
	this.salary = salary;
}
public Employee(String name, String unit, Double salary) {
	super();
	this.name = name;
	this.unit = unit;
	this.salary = salary;
}
@Override
public String toString() {
	return "Employee [name=" + name + ", unit=" + unit + ", salary=" + salary + "]";
}

}
