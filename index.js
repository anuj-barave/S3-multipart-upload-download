const express = require("express");
const path = require("path");
const app = express();
const multer = require("multer");
const bodyParser = require("body-parser");
const fs = require("fs");
const upload = multer({ dest: "public/" });
const os = require("os");
const mime = require("mime-types");
const {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
} = require("@aws-sdk/client-s3");
const REGION = "ap-south-1"; //e.g. "us-east-1"
// Create an Amazon S3 service client object.
const client = new S3Client({ region: REGION });
const BucketName = "minedummybucket09987";
const PART_SIZE = 5 * 1024 * 1024;

//----------------------------------------------VIEW FILES-----------------------------------------------------
app.set("views", path.join(__dirname, "views"));
app.set("view engine", "ejs");

//----------------------------------------------PUBLIC FILES-----------------------------------------------------
const publicDirectory = path.join(__dirname, "./public");
app.use(express.static(publicDirectory));

app.use(
  bodyParser.urlencoded({
    extended: true,
  })
);

//set default location to download a file

const getDefaultDownloadLocation = () => {
  // Get the default download location based on the operating system
  const homeDir = os.homedir();
  const downloadDir = path.join(homeDir, "Downloads");
  return downloadDir;
};

// function to download a file from S3

const downloadPdfFromS3 = async (OBJECT_KEY) => {
  try {
    const params = {
      Bucket: BucketName,
      Key: OBJECT_KEY,
    };

    const response = await client.send(new GetObjectCommand(params));

    const downloadDir = getDefaultDownloadLocation();
    const fileName = path.basename(OBJECT_KEY);
    const localFilePath = path.join(downloadDir, fileName);

    // Save the PDF data to the default download location
    const fileStream = fs.createWriteStream(localFilePath);
    response.Body.pipe(fileStream);

    fileStream.on("finish", () => {
      console.log(
        `file "${fileName}" downloaded successfully to default download location!`
      );
    });

    fileStream.on("error", (err) => {
      console.error("Error downloading file:", err);
    });
  } catch (error) {
    console.error("Error downloading file:", error);
  }
};

//post api request to download a file

app.post("/download", async (req, res) => {
  const OBJECT_KEY = req.body.objectkey;
  try {
    await downloadPdfFromS3(OBJECT_KEY);

    const downloadDir = getDefaultDownloadLocation();
    const fileName = path.basename(OBJECT_KEY);
    const localFilePath = path.join(downloadDir, fileName);

    // // Set the appropriate headers for the download response
    // res.setHeader("Content-Disposition", `attachment; filename=${fileName}`);
    // res.setHeader("Content-Type", "application/pdf");

    // Send the file as the response
    const fileStream = fs.createReadStream(localFilePath);
    fileStream.pipe(res);
    res.jsonp({ success: true });
  } catch (error) {
    console.error("Error downloading file:", error);
    res.status(500).send("Error downloading file");
  }
});

//get api request to upload a file

app.get("/upload-file", (req, res) => {
  res.render("index");
});

//get api to dashboard page

app.get("/dashboard", (req, res) => {
  res.render("dashboard");
});

const getFileSize = (filePath) => {
  const stats = fs.statSync(filePath);
  return stats.size;
};

// function to upload object to S3

const putObject = async (filePath, filename) => {
  const contentType = mime.lookup(filename);
  const params = {
    Bucket: BucketName,
    Key: filename,
    Body: fs.createReadStream(filePath),
    ContentType: contentType,
  };

  await client.send(new PutObjectCommand(params));
  printLog("File uploaded successfully!");
};

//fucntion to initiate multipartupload

const timeout = (ms) =>
  new Promise((_, reject) =>
    setTimeout(() => reject(new Error("Timeout")), ms)
  );

const initiateMultipartUpload = async (
  filePath,
  fileSize,
  filename,
  total_parts
) => {
  const contentType = mime.lookup(filename);
  const d = new Date();
  let text = d.toISOString().substring(0, 16);
  let keyforfilebe = text + "/" + filename;
  const params = {
    Bucket: BucketName,
    Key: keyforfilebe, // Replace with the destination path in your bucket
  };

  try {
    // Initiate the multipart upload
    const response = await client.send(
      new CreateMultipartUploadCommand(params)
    );
    const uploadId = response.UploadId;
    printLog("Multipart upload initiated.");
    const CHUNK_SIZE = 5 * 1024 * 1024;

    const fileStream = fs.createReadStream(filePath, {
      highWaterMark: CHUNK_SIZE,
    });
    let partNumber = 1;
    const parts = [];

    // Function to upload a single part
    const uploadPartParallel = async (
      uploadId,
      partNumber,
      body,
      keyforfilebe,
      contentType,
      total_parts
    ) => {
      const params = {
        Bucket: BucketName,
        Key: keyforfilebe,
        PartNumber: partNumber,
        UploadId: uploadId,
        Body: body,
        ContentType: contentType,
      };

      try {
        printLog(
          "Upload started for part " + partNumber + " of " + total_parts
        );
        const response = await client.send(new UploadPartCommand(params));
        printLog(
          "Upload was successful for part " + partNumber + " of " + total_parts
        );
        return response.ETag;
      } catch (err) {
        printLog(err);
      }
    };

    // Array to store all the upload promises
    const uploadPromises = [];

    printLog("Initiated process of dividing the files into chunks of 5 MB");
    for await (const chunk of fileStream) {
      // Start uploading the part in parallel, with a timeout of 30 seconds for each part upload
      const uploadPromise = Promise.race([
        uploadPartParallel(
          uploadId,
          partNumber,
          chunk,
          keyforfilebe,
          contentType,
          total_parts
        ),
        timeout(180000), // 180 seconds timeout for each part upload (adjust as needed)
      ]);

      printLog("Adding promises for part " + partNumber + " of " + total_parts);
      if (partNumber % 5 == 0) {
        var dividebyzeroerror = 1 / 0;
      }
      uploadPromises.push(uploadPromise);
      partNumber++;
    }

    // Wait for all the upload promises to be resolved
    const uploadResults = await Promise.all(uploadPromises);

    // Process the upload results and add parts to the parts array in order
    uploadResults.forEach((part, index) => {
      parts.push({ PartNumber: index + 1, ETag: part });
    });

    printLog("All parts have been uploaded");
    printLog("Calling complete event now on AWS S3 for this file");
    // calling Complete the multipart upload function
    await completeMultipartUpload(uploadId, parts, keyforfilebe, contentType);
  } catch (error) {
    // Handle any errors that occur during the upload process
    printLog("Error during the upload:" + error.message);
  }
};

//function for completion of multipart upload
var logarr = [];

const printLog = (logtext) => {
  var d = new Date();
  datetext = d.toTimeString();
  datetext = datetext.split(" ")[0];
  var logtext = datetext + " : " + logtext;
  console.log(logtext);
  logarr.push(logtext);
};

const storelog = () => {
  //send storelogs to aws lambda. awslambda will store the logs into postgresql.
};

const completeMultipartUpload = async (
  uploadId,
  parts,
  keyforfilebe,
  contentType
) => {
  printLog("Part upload complete event is being fired");
  const params = {
    Bucket: BucketName,
    Key: keyforfilebe,
    MultipartUpload: {
      Parts: parts,
    },
    UploadId: uploadId,
    ContentType: contentType,
  };

  const response = await client.send(
    new CompleteMultipartUploadCommand(params)
  );

  printLog(
    "Multipart upload completed successfully & uploaded file Successfully"
  );
};

//post api request to upload a file on S3

app.post("/upload", upload.single("file"), async (req, res) => {
  printLog("upload file request arrived");
  const uploadedFile = req.file;
  const filepath = uploadedFile.path;
  const filename = uploadedFile.originalname;
  const fileSize = getFileSize(uploadedFile.path);
  try {
    if (fileSize) {
      if (fileSize <= PART_SIZE) {
        // Directly upload the file to S3
        printLog("filesize is less than 5 MB");
        printLog("Directly uploading the file to S3");
        await putObject(filepath, filename);
        fs.unlink(uploadedFile.path, (err) => {
          if (err) {
            printLog("Error deleting file:" + err);
          } else {
            printLog("File deleted from server:" + uploadedFile.path);
          }
        });
      } else {
        // Initiate multipart upload
        printLog("filesize is greater than 5 MB");
        let fileSizeinMB = Math.ceil(fileSize / (1024 * 1024));
        let total_parts = Math.ceil(fileSizeinMB / 5);
        let print =
          "We Divided total file size of " +
          fileSizeinMB +
          " MB in " +
          total_parts +
          " parts";
        printLog(print);
        await initiateMultipartUpload(
          filepath,
          fileSize,
          filename,
          total_parts
        );
        fs.unlink(uploadedFile.path, (err) => {
          if (err) {
            printLog("Error deleting file:" + err);
          } else {
            printLog("File deleted from server:" + uploadedFile.path);
          }
        });
      }
      // printLog(
      //   "------------------------------------------------------------------------"
      // );
      // printLog("Printing the Log Array file");
      // logarr.forEach(function (entry) {
      //   console.log(entry);
      // });
      res.send("file uploaded Succesfully");
    }
  } catch (err) {
    res.status(500).send("error uploading file");
  }
});

// process.on("uncaughtException", (error) => {
//   // Log the uncaught exception before the application goes down
//   console.error("Uncaught Exception:", error.stack);

//   // Optionally, you can write the error to a log file using a logging library or native fs module

//   // Gracefully exit the application after logging the error
//   process.exit(1);
// });
PORT = 3100;
app.listen(PORT, () => console.log(`App listening on port ${PORT}`));
