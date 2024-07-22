package com.ds.mapping;

import java.util.*;

import com.ds.mapping.entity.Address;
import com.ds.mapping.entity.Books;
import com.ds.mapping.entity.Student;
import com.ds.mapping.entity.details;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
import jakarta.persistence.Persistence;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	EntityManagerFactory entityManagerFactory = Persistence.createEntityManagerFactory("demo");

		EntityManager entityManager = entityManagerFactory.createEntityManager();

		EntityTransaction transaction = entityManager.getTransaction();
		
		Student s=new Student();
		s.setFullName("John Doe");
		s.setAge(22);
		
		Books b=new Books();
		b.setId(45);
		b.setName("tvd");
		b.getStudents().add(s);
		
		details info = new details();
		info.setAadhar("321321321323");
		info.setPassport("hjsda23123");
		
		Address address = new Address();
		address.setStreet("sadas");
		address.setState("asdasd");
		address.setPin(211212);
		address.setS(s);
		
		Address address1 = new Address();
		address1.setStreet("saadssdas");
		address1.setState("asdasdssd");
		address1.setPin(2132212);
		address1.setS(s);
		
		List<Address> addresses = new ArrayList<Address>();
		addresses.add(address);
		addresses.add(address1);
		
		s.setAddresses(addresses);
		
		s.setInfo(info);
		info.setS(s);
		
		transaction.begin();
		entityManager.persist(s);
		transaction.commit();
		
    }
}
