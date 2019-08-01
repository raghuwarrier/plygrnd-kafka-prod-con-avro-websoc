package com.kafka.avroschemagen.model;

import java.util.List;

public class Employee {
	private String name;
	private int age;
	private List<String> addressLines;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public List<String> getAddressLines() {
		return addressLines;
	}

	public void setAddressLines(List<String> addressLines) {
		this.addressLines = addressLines;
	}
}
