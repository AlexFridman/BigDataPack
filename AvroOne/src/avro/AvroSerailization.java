package avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

public class AvroSerailization {
	public static void main(String[] args) {
		driver d1 = new driver();
		d1.setName("Michael");
		d1.setAge(34);
		d1.setAddress("Chicago");
		d1.setHours(23);
		d1.setId(12);

		driver d2 = new driver();
		d2.setName("Kevin");
		d2.setAge(34);
		d2.setAddress("Chicago");
		d2.setHours(2);
		d2.setId(1);

		DatumWriter<driver> driverWriter = new SpecificDatumWriter<driver>(driver.class);
		DataFileWriter<driver> driverFileWriter = new DataFileWriter<driver>(driverWriter);

		try {
			driverFileWriter.create(d1.getSchema(), new File("/opt/driver.avro"));
			driverFileWriter.append(d1);
	driverFileWriter.append(d2);
			driverFileWriter.close();

		} catch (IOException e) {
			System.out.println("Exceptoin while serailaing data");
			e.printStackTrace();
		}

		System.out.println("Completed Serializing data");
	}
}
