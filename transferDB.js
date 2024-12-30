import config from "config";
import { exec } from "child_process";
import fs from "fs";

const sourcePg = config.get("source.pgConnectionString");
const destinationPg = config.get("destination.pgConnectionString");
const exportDir = "exports";

// Utility to run shell commands
function runCommand(command) {
  return new Promise((resolve, reject) => {
    exec(command, (error, stdout, stderr) => {
      if (error) {
        reject(`Error: ${error.message}`);
        return;
      }
      if (stderr) {
        console.error(`Stderr: ${stderr}`);
      }
      resolve(stdout);
    });
  });
}

// Function to get all tables in a schema
async function getTablesForSchema(schema) {
  const query = `
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = '${schema}' AND table_type = 'BASE TABLE';
  `;
  const command = `psql ${sourcePg} -t -c "${query}"`;
  console.log(`Fetching tables for schema ${schema}...`);
  const result = await runCommand(command);
  const tables = result
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  console.log(`Tables in schema ${schema}: ${tables.join(", ")}`);
  return tables;
}

// Function to export a table
async function exportTable(schema, table) {
  const exportFile = `${exportDir}/${schema}_${table}.csv`;
  const tableName = `${schema}.${table}`;

  // Check if table exists
  const checkTableCommand = `psql ${sourcePg} -t -c "\\dt ${tableName}"`;
  const tableExists = await runCommand(checkTableCommand).catch(() => null);

  if (!tableExists) {
    console.error(`Table ${tableName} does not exist. Skipping export.`);
    return;
  }

  const command = `psql ${sourcePg} -c "\\COPY ${tableName} TO '${exportFile}' WITH CSV HEADER;"`;
  console.log(`Exporting data for table ${tableName}...`);
  await runCommand(command);
  console.log(`Table ${tableName} exported to ${exportFile}`);
}

// Function to import a table
async function importTable(schema, table) {
  const importFile = `${exportDir}/${schema}_${table}.csv`;
  const tableName = `${schema}.${table}`;

  // Check if table exists in destination
  const checkTableCommand = `psql ${destinationPg} -t -c "\\dt ${tableName}"`;
  const tableExists = await runCommand(checkTableCommand).catch(() => null);

  if (!tableExists) {
    console.error(`Table ${tableName} does not exist. Skipping import.`);
    return;
  }

  if (!fs.existsSync(importFile)) {
    console.error(`File ${importFile} does not exist. Skipping import.`);
    return;
  }

  const command = `
    psql ${destinationPg} -c "
      SET session_replication_role = 'replica';
      TRUNCATE ${tableName} RESTART IDENTITY CASCADE;
      \\COPY ${tableName} FROM '${importFile}' WITH CSV HEADER;
      SET session_replication_role = 'origin';
    ";
  `;
  console.log(`Importing data for table ${tableName}...`);
  await runCommand(command);
  console.log(`Table ${tableName} imported from ${importFile}`);
}

// Main function
async function main() {
  try {
    const schemas = ["storage", "public", "auth"];

    // Ensure export directory exists
    if (!fs.existsSync(exportDir)) {
      fs.mkdirSync(exportDir);
    }

    // Process each schema
    for (const schema of schemas) {
      const tables = await getTablesForSchema(schema);

      // Export tables
      for (const table of tables) {
        await exportTable(schema, table);
      }

      // Import tables
      for (const table of tables) {
        await importTable(schema, table);
      }
    }

    console.log("All operations completed successfully.");
  } catch (error) {
    console.error(`Error: ${error}`);
  }
}

// Execute the script
main();
