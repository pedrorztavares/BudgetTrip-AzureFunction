const { app } = require("@azure/functions");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  DynamoDBDocumentClient,
  ScanCommand,
  QueryCommand,
  DeleteCommand,
} = require("@aws-sdk/lib-dynamodb");

const dynamoClient = new DynamoDBClient({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const dynamo = DynamoDBDocumentClient.from(dynamoClient);

function isOlderThanRetention(createdAt, retentionDays) {
  if (!createdAt) return false;

  const createdDate = new Date(createdAt);

  if (Number.isNaN(createdDate.getTime())) {
    return false;
  }

  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - retentionDays);

  return createdDate < cutoffDate;
}

async function deleteExpensesForTrip(expensesTable, tripId, context) {
  let lastEvaluatedKey = undefined;
  let deletedExpensesCount = 0;

  do {
    const result = await dynamo.send(
      new QueryCommand({
        TableName: expensesTable,
        KeyConditionExpression: "tripId = :tripId",
        ExpressionAttributeValues: {
          ":tripId": tripId,
        },
        ExclusiveStartKey: lastEvaluatedKey,
      })
    );

    const expenses = result.Items || [];

    for (const expense of expenses) {
      await dynamo.send(
        new DeleteCommand({
          TableName: expensesTable,
          Key: {
            tripId: expense.tripId,
            id: expense.id,
          },
        })
      );

      deletedExpensesCount++;
    }

    lastEvaluatedKey = result.LastEvaluatedKey;
  } while (lastEvaluatedKey);

  context.log(
    `Deleted ${deletedExpensesCount} expenses from trip ${tripId}`
  );

  return deletedExpensesCount;
}

app.timer("deleteOldGroups", {
  schedule: "0 0 2 * * *",

  handler: async (timer, context) => {
    context.log("Starting old groups cleanup function...");

    const TRIPS_TABLE = process.env.DYNAMODB_TABLE_TRIPS;
    const EXPENSES_TABLE = process.env.DYNAMODB_TABLE_EXPENSES;
    const retentionDays = Number(process.env.GROUP_RETENTION_DAYS || 60);

    if (!TRIPS_TABLE) {
      throw new Error("Missing DYNAMODB_TABLE_TRIPS environment variable");
    }

    if (!EXPENSES_TABLE) {
      throw new Error("Missing DYNAMODB_TABLE_EXPENSES environment variable");
    }

    if (!retentionDays || retentionDays <= 0) {
      throw new Error("Invalid GROUP_RETENTION_DAYS value");
    }

    let lastEvaluatedKey = undefined;
    let checkedGroupsCount = 0;
    let deletedGroupsCount = 0;
    let deletedExpensesTotal = 0;

    do {
      const result = await dynamo.send(
        new ScanCommand({
          TableName: TRIPS_TABLE,
          ExclusiveStartKey: lastEvaluatedKey,
        })
      );

      const trips = result.Items || [];

      for (const trip of trips) {
        checkedGroupsCount++;

        if (!trip.id) {
          context.log("Skipping trip without id:", trip);
          continue;
        }

        if (!isOlderThanRetention(trip.createdAt, retentionDays)) {
          continue;
        }

        context.log(
          `Deleting old trip ${trip.id} created at ${trip.createdAt}`
        );

        const deletedExpensesCount = await deleteExpensesForTrip(
          EXPENSES_TABLE,
          trip.id,
          context
        );

        deletedExpensesTotal += deletedExpensesCount;

        await dynamo.send(
          new DeleteCommand({
            TableName: TRIPS_TABLE,
            Key: {
              id: trip.id,
            },
          })
        );

        deletedGroupsCount++;

        context.log(`Deleted trip ${trip.id}`);
      }

      lastEvaluatedKey = result.LastEvaluatedKey;
    } while (lastEvaluatedKey);

    context.log("Old groups cleanup completed.");
    context.log(`Checked groups: ${checkedGroupsCount}`);
    context.log(`Deleted groups: ${deletedGroupsCount}`);
    context.log(`Deleted related expenses: ${deletedExpensesTotal}`);
  },
});
