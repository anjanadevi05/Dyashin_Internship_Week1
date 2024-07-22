package com.ds.mapping.entity;
import java.util.*;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Table;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.OneToMany;
import jakarta.persistence.OneToOne;

@Entity
@Table(name="stud_table")
public class Student {
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
    private int id;
	
	@Column(name = "name")
	private String fullName;

	private int age;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getFullName() {
		return fullName;
	}

	public void setFullName(String fullName) {
		this.fullName = fullName;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}
	@OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY, mappedBy = "s")
	private details info;
	
	public details getInfo() {
		return info;
	}

	public void setInfo(details info) {
		this.info = info;
	}

	public List<Address> getAddresses() {
		return addresses;
	}

	public void setAddresses(List<Address> addresses) {
		this.addresses = addresses;
	}
	@OneToMany(cascade = CascadeType.ALL,fetch = FetchType.LAZY, mappedBy = "s")
	private List<Address> addresses;
	
	@ManyToMany(cascade = CascadeType.ALL,fetch = FetchType.LAZY)
	private List<Books>book=new ArrayList<Books>();

}
