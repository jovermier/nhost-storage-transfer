import { NhostClient } from "@nhost/nhost-js";
import config from "config";
import pRetry, { AbortError } from "p-retry";
import axios from "axios";
import FormData from "form-data";

const sourceConfig = config.get("source.nhost");
const sourceNhost = new NhostClient(sourceConfig);

const destinationConfig = config.get("destination.nhost");
const destinationNhost = new NhostClient(destinationConfig);

const sleepTimeBetweenTransfers =
  config.get("sleepTimeBetweenTransfers") ?? 1000; // 1 second

// GraphQL query to fetch all files
const query = `
{
  files {
    id
    bucketId
    createdAt
    updatedAt
    name
    size
    mimeType
    etag
    isUploaded
    uploadedByUserId
  }
}
`;

// Example of how to transfer a file from one Nhost project to another
// https://github.com/nhost/hasura-storage/blob/main/example_curl.sh#L21C7-L21C7

const transferFile = async (url, { id, name, bucketId, ...metadata }) => {
  // fetch the file data from the URL using axios
  const response = await axios.get(url, { responseType: "stream" });

  const formData = new FormData();

  const adjustedMetadata = {
    id,
    name: name,
    createdAt: metadata.createdAt,
    updatedAt: metadata.updatedAt,
    size: metadata.size,
    mimeType: metadata.mimeType,
    etag: metadata.etag,
    uploadedByUserId: metadata.uploadedByUserId,
  };

  formData.append("file[]", response.data, name);
  formData.append("metadata[]", JSON.stringify(adjustedMetadata), {
    contentType: "application/json",
  });

  const res = await destinationNhost.storage.upload({
    formData,
    id,
    name,
    bucketId,
  });
  if (res.error) {
    console.log(res.error.message);
  }

  return res.fileMetadata;
};

const uploadUrl = `${destinationNhost.storage.url}/files`;

const transferFileAxios = async (url, { id, bucketId, ...metadata }) => {
  const response = await axios.get(url, { responseType: "stream" });

  const fileData = await new Promise((resolve, reject) => {
    const chunks = [];
    response.data
      .on("data", (chunk) => {
        chunks.push(chunk);
      })
      .on("end", () => {
        resolve(Buffer.concat(chunks));
      })
      .on("error", reject); // Make sure you handle potential errors from the stream
  });

  const formData = new FormData();

  // Adjust the metadata as you did before
  const adjustedMetadata = {
    id,
    name: metadata.name,
    createdAt: metadata.createdAt,
    updatedAt: metadata.updatedAt,
    size: metadata.size,
    mimeType: metadata.mimeType,
    etag: metadata.etag,
    uploadedByUserId: metadata.uploadedByUserId,
  };

  formData.append("file[]", fileData, metadata.name);
  formData.append("metadata[]", JSON.stringify(adjustedMetadata), {
    contentType: "application/json",
  });

  try {
    const uploadResponse = await axios.post(uploadUrl, formData, {
      headers: {
        ...formData.getHeaders(),
        "x-hasura-admin-secret": destinationConfig.adminSecret,
      },
    });

    if (uploadResponse.status !== 200) {
      console.log(uploadResponse.data.message || "File upload failed");
    }

    return uploadResponse;
  } catch (e) {
    console.log("Error message:", e.response.data.error);
  }
};

const transferSingleFile = async (file) => {
  try {
    const presignedUrlRes = await sourceNhost.storage.getPresignedUrl({
      fileId: file.id,
    });
    const url = presignedUrlRes.presignedUrl.url;
    if (!url) {
      throw new Error(`Failed to get pre-signed URL for file ${file.id}`);
    }

    await transferFile(url, file);
    console.log(`Transferred file ${file.name} with id ${file.id}`);
  } catch (e) {
    console.log(`Error while transferring ${file.name}:`, e);

    // If you have a certain error condition on which you'd like to stop retries:
    if (e.message === "SomeSpecificErrorMessage") {
      throw new AbortError(e.message);
    }

    throw e; // Otherwise, throw the error to let pRetry handle retries
  }
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function transferFiles() {
  // Fetch files from destination
  const destinationResponse = await sourceNhost.graphql.request(query);
  const destinationFiles = destinationResponse.data.files;

  for (const file of destinationFiles) {
    await pRetry(() => transferSingleFile(file), {
      retries: 5,
      onFailedAttempt: (error) => {
        console.log(
          `Attempt ${error.attemptNumber} failed. There are ${error.retriesLeft} retries left.`
        );
      },
    });
    await sleep(sleepTimeBetweenTransfers); // ms
  }
}

transferFiles();
