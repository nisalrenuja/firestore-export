const { GoogleAuth } = require("google-auth-library");
const fetch = require("node-fetch");
const { Storage } = require("@google-cloud/storage");
const fs = require("fs");
const path = require("path");

// Configuration: replace these with your values
const keyFilePath = "path/to/your-service-account-key.json"; // your service account key file
const projectId = "your-project-id"; // your Google Cloud project ID
const bucketName = "your-bucket-name"; // your Google Cloud Storage bucket name
const outputUriPrefix = `gs://${bucketName}/your-specified-path/`; // GCS path for exported files

// Google Cloud clients
const auth = new GoogleAuth({
  keyFile: keyFilePath,
  scopes: [
    "https://www.googleapis.com/auth/datastore",
    "https://www.googleapis.com/auth/cloud-platform",
  ],
});
const storage = new Storage({ keyFilename: keyFilePath });

// Function to trigger Firestore export
async function exportFirestore() {
  const accessToken = (await auth.getAccessToken()).token;
  const url = `https://firestore.googleapis.com/v1/projects/${projectId}/databases/(default):exportDocuments`;
  const body = JSON.stringify({ outputUriPrefix });

  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json",
    },
    body: body,
  });

  const responseData = await response.json();

  if (response.ok) {
    console.log("Export operation started:", responseData);
    return responseData.name; // Return the operation name
  } else {
    console.error("Error starting export operation:", responseData);
    throw new Error("ExportOperationFailed");
  }
}

// Function to monitor the operation's status
async function monitorOperation(operationName) {
  const accessToken = (await auth.getAccessToken()).token;
  const url = `https://firestore.googleapis.com/v1/${operationName}`;

  while (true) {
    const response = await fetch(url, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
      },
    });

    const responseData = await response.json();

    if (response.ok) {
      const { done } = responseData;
      if (done) {
        console.log("Operation complete:", responseData);
        return;
      }
      console.log("Operation in progress, waiting...");
      await new Promise((resolve) => setTimeout(resolve, 10000)); // Polling every 10 seconds
    } else {
      console.error("Error checking operation status:", responseData);
      throw new Error("OperationMonitoringFailed");
    }
  }
}

// Function to download exported files
async function downloadExportedFiles() {
  const destPath = "./downloads";
  fs.mkdirSync(destPath, { recursive: true }); // Create the directory if doesn't exist

  const [files] = await storage
    .bucket(bucketName)
    .getFiles({ prefix: "your-specified-path/" }); // specify the path in GCS
  for (const file of files) {
    const destination = path.join(destPath, file.name);
    await file.download({ destination });
    console.log(`Downloaded ${file.name} to ${destination}`);
  }
}

// Main function to execute the script logic
async function main() {
  try {
    const operationName = await exportFirestore();
    await monitorOperation(operationName);
    await downloadExportedFiles();
    console.log("Firestore export complete!");
  } catch (error) {
    console.error("An error occurred:", error);
  }
}

main();
