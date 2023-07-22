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
  console.log("File uploaded successfully!");
};

//fucntion to initiate multipartupload

const initiateMultipartUpload = async (filePath, fileSize, filename) => {
  const params = {
    Bucket: BucketName,
    Key: filename, // Replace with the destination path in your bucket
  };

  const response = await client.send(new CreateMultipartUploadCommand(params));
  const uploadId = response.UploadId;
  console.log("Multipart upload initiated.");
  const CHUNK_SIZE = 5 * 1024 * 1024;

  const fileStream = fs.createReadStream(filePath, {
    highWaterMark: CHUNK_SIZE,
  });
  let partNumber = 1;
  const parts = [];

  // Function to upload a single part
  const uploadPartParallel = async (uploadId, partNumber, body, filename) => {
    const contentType = mime.lookup(filename);
    const params = {
      Bucket: BucketName,
      Key: filename,
      PartNumber: partNumber,
      UploadId: uploadId,
      Body: body,
      ContentType: contentType,
    };
    try {
      const response = await client.send(new UploadPartCommand(params));
      console.log("Uploaded ", partNumber, " part on S3");
      return response.ETag;
    } catch (err) {
      console.log(err);
    }
  };

  // Array to store all the upload promises
  const uploadPromises = [];

  for await (const chunk of fileStream) {
    // Start uploading the part in parallel
    const uploadPromise = uploadPartParallel(
      uploadId,
      partNumber,
      chunk,
      filename
    );
    uploadPromises.push(uploadPromise);
    partNumber++;
  }

  // Wait for all the upload promises to be resolved
  const uploadResults = await Promise.all(uploadPromises);

  // Process the upload results and add parts to the parts array in order
  uploadResults.forEach((part, index) => {
    parts.push({ PartNumber: index + 1, ETag: part });
  });

  // Complete the multipart upload
  await completeMultipartUpload(uploadId, parts, filename);
};

//function for completion of multipart upload

const completeMultipartUpload = async (uploadId, parts, filename) => {
  const contentType = mime.lookup(filename);
  const params = {
    Bucket: BucketName,
    Key: filename,
    MultipartUpload: {
      Parts: parts,
    },
    UploadId: uploadId,
    ContentType: contentType,
  };

  await client.send(new CompleteMultipartUploadCommand(params));
  console.log("Multipart upload completed successfully!");
};

//post api request to upload a file on S3

app.post("/upload", upload.single("file"), async (req, res) => {
  console.log("upload file request arrived");
  const uploadedFile = req.file;
  const filepath = uploadedFile.path;
  const filename = uploadedFile.originalname;

  const fileSize = getFileSize(uploadedFile.path);
  try {
    if (fileSize) {
      if (fileSize <= PART_SIZE) {
        // Directly upload the file to S3
        console.log("Directly uploading the file to S3");
        await putObject(filepath, filename);
        fs.unlink(uploadedFile.path, (err) => {
          if (err) {
            console.error("Error deleting file:", err);
          } else {
            console.log("File deleted from server:", uploadedFile.path);
          }
        });
      } else {
        // Initiate multipart upload
        await initiateMultipartUpload(filepath, fileSize, filename);
        fs.unlink(uploadedFile.path, (err) => {
          if (err) {
            console.error("Error deleting file:", err);
          } else {
            console.log("File deleted from server:", uploadedFile.path);
          }
        });
      }
      res.send("file uploaded Succesfully");
    }
  } catch (err) {
    res.status(500).send("error uploading file");
  }
});

PORT = 3100;
app.listen(PORT, () => console.log(`App listening on port ${PORT}`));
