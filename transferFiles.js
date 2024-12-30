import { NhostClient } from "@nhost/nhost-js";
import config from "config";
import pRetry from "p-retry";
import axios from "axios";
import FormData from "form-data";
import c from "config";

const sourceConfig = config.get("source.nhost");
const sourceNhost = new NhostClient(sourceConfig);

const destinationConfig = config.get("destination.nhost");
const destinationNhost = new NhostClient(destinationConfig);

const sleepTimeBetweenTransfers =
  config.get("sleepTimeBetweenTransfers") ?? 1000; // 1 second

const retries = config.get("retries") ?? 2;

// GraphQL query to fetch all files
const allFilesQuery = `
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
    metadata
  }
}
`;

const uniqueViolationStr =
  "Uniqueness violation. duplicate key value violates unique constraint";

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
    metadata: metadata.metadata,
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
    const url = presignedUrlRes?.presignedUrl?.url;
    if (!url) {
      throw new Error(`Failed to get pre-signed URL for file ${file.id}`);
    }

    await transferFile(url, file);
  } catch (e) {
    const message = e.message.startsWith(uniqueViolationStr)
      ? uniqueViolationStr
      : e.message;
    console.log(`Error while transferring ${file.name}:`, message);

    // If you have a certain error condition on which you'd like to stop retries:
    if (e.message.startsWith(uniqueViolationStr)) {
      return;
    }

    throw e; // Otherwise, throw the error to let pRetry handle retries
  }
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function transferFiles() {
  // Fetch files from destination
  const sourceRes = await sourceNhost.graphql.request(allFilesQuery);
  const sourceFiles = sourceRes.data.files;

  const destinationRes = await destinationNhost.graphql.request(allFilesQuery);
  const destinationFiles = destinationRes.data.files;

  const filesNeedingUpload = sourceFiles.filter((sourceFile) => {
    return !destinationFiles.some(
      (destinationFile) => destinationFile.id === sourceFile.id
    );
  });

  const filesNeedingRemoval = destinationFiles.filter((destinationFile) => {
    return !sourceFiles.some(
      (sourceFile) => sourceFile.id === destinationFile.id
    );
  });

  let count = 0;
  for (const file of filesNeedingUpload) {
    // // check if the file is already in the destination
    // const destinationFile = await destinationNhost.storage.download({
    //   fileId: file.id,
    // });

    // if (!!destinationFile.file) {
    //   continue;
    // }

    count++;
    await pRetry(() => transferSingleFile(file), {
      retries,
      onFailedAttempt: (error) => {
        console.log(
          `Attempt ${error.attemptNumber} failed. There are ${error.retriesLeft} retries left.`
        );
      },
    }).then(() => {
      console.log(`${count} Transferred file ${file.name} with id ${file.id}`);
    });
    await sleep(sleepTimeBetweenTransfers); // ms
  }

  // use a single graphql request to delete all files
  try {
    const idsToDelete = filesNeedingRemoval.map((file) => file.id);
    const deleteResponse = await destinationNhost.graphql.request(
      `mutation($ids: [uuid!]!) {
          deleteFiles(where: {id: {_in: $ids}}) {
            affected_rows
          }
        }`,
      {
        ids: idsToDelete,
      }
    );
    console.log(
      `Deleted ${deleteResponse.data.deleteFiles.affected_rows} files`
    );
  } catch (e) {
    console.log(`Error deleting files:`, e.message);
  }
}
