package com.example.app.app;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.ListBlobContainersOptions;
import com.azure.storage.common.StorageSharedKeyCredential;

@SpringBootApplication
public class AppApplication {

	public static void main(String[] args) {
		SpringApplication.run(AppApplication.class, args);
		String accountName = "adlsconnectiontest0";
       String accountKey = "V2jesgxcIbx4mN+pgfdKoEP4rHgB/lpSci32XmU64LSACJmdQGTj60Ly7Sb/xckiElbftxNJJAge+AStmiTBJA==";
	   String containername = "test-data";
       String filePath = "creditcardtest.csv";
       String destinationPath = "output.json"; // Destination file path for JSON file
       BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
             .endpoint("https://" + accountName + ".blob.core.windows.net")
             .credential(new StorageSharedKeyCredential(accountName, accountKey))
             .buildClient();

       // List all the containers in the storage
       ListBlobContainersOptions options = new ListBlobContainersOptions()
             .setPrefix(""); // You can filter using the prefix

       System.out.println("List containers:");
       for (BlobContainerItem blobContainerItem : blobServiceClient.listBlobContainers(options, null)) {
          System.out.printf("Container name: %s%n", blobContainerItem.getName());
       }

       BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containername);
       BlobClient blobClient = containerClient.getBlobClient(filePath);

       List<String[]> data = new ArrayList<>(); // Data structure to store CSV data
       try (InputStream inputStream = blobClient.openInputStream();
           FileOutputStream outputStream = new FileOutputStream(destinationPath)) {

          byte[] buffer = new byte[8192];
          int bytesRead;
          while ((bytesRead = inputStream.read(buffer)) != -1) {
             outputStream.write(buffer, 0, bytesRead);
          }

          System.out.println("File downloaded successfully as JSON: " + destinationPath);
       } catch (IOException e) {
          System.out.println("Error downloading the file: " + e.getMessage());
       }

       try (InputStream inputStream = blobClient.openInputStream();
           Scanner scanner = new Scanner(inputStream)) {

          if (scanner.hasNextLine()) {
             scanner.nextLine();
             System.out.println();
          }

          while (scanner.hasNextLine()) {
             String line = scanner.nextLine();
             String[] columns = line.split(",");
             data.add(columns);
          }
       } catch (IOException e) {
          System.out.println("Error reading the file: " + e.getMessage());
       }

       for (String[] row : data) {
          for (String column : row) {
             System.out.print(column + " ");
          }
          System.out.println();
       }
	}

}
