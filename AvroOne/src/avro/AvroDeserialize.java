package avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

public class AvroDeserialize {
	public static void main(String[] args) {
		DatumReader<driver> driverReader = new SpecificDatumReader<driver>(driver.class);
		driver dr = null;

		try {
			DataFileReader<driver> dataFileReader = new DataFileReader<driver>(new File("/opt/driver.avro"),
					driverReader);
			while (dataFileReader.hasNext()) {
				dr = dataFileReader.next();
				System.out.println(dr);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
