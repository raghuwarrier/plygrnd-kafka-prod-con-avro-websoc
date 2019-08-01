package com.kafka.avroschemagen.example;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;


import com.kafka.avroschemagen.model.Employee;
import com.kafka.avroschemagen.model.ListOfEmployees;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.Nullable;

import javax.validation.constraints.Max;


public class AvroSchemaGen {

	public static class Packet {
		int cost;
		@Nullable TimeStamp stamp;
		public Packet() {}                        // required to read
		public Packet(int cost, TimeStamp stamp){
			this.cost = cost;
			this.stamp = stamp;
		}
	}

	public static class TimeStamp {
		int hour = 0;
		int second = 0;
		public TimeStamp() {}                     // required to read
		public TimeStamp(int hour, int second){
			this.hour = hour;
			this.second = second;
		}
	}

	public static void main(String[] args) throws Exception {
		// one argument: a file name
		File file = new File(args[0]);

		// get the reflected schema for packets
		Schema schema = ReflectData.get().getSchema(ListOfEmployees.class);
		OutputStream os = new FileOutputStream(new File("C:\\users\\wa2126\\workarea\\avrogenschema-listofemployees.avsc"));
		os.write(schema.toString(true).getBytes("UTF8"));
		os.flush();
		os.close();
		// create a file of packets
		DatumWriter<ListOfEmployees> writer = new ReflectDatumWriter<ListOfEmployees>(ListOfEmployees.class);
		DataFileWriter<ListOfEmployees> out = new DataFileWriter<ListOfEmployees>(writer)
				  .setCodec(CodecFactory.deflateCodec(9))
				  .create(schema, file);
		out.flush();
		out.close();

/*
		// write 100 packets to the file, odds with null timestamp
		for (int i = 0; i < 100; i++) {
			out.append(new Packet(i, (i%2==0) ? new TimeStamp(12, i) : null));
		}

		// close the output file
		out.close();

		// open a file of packets
		DatumReader<Packet> reader = new ReflectDatumReader<Packet>(Packet.class);
		DataFileReader<Packet> in = new DataFileReader<Packet>(file, reader);

		// read 100 packets from the file & print them as JSON
		for (Packet packet : in) {
			System.out.println(ReflectData.get().toString(packet));
		}

		// close the input file
		in.close();
*/
	}

}
