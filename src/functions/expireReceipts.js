const { app } = require("@azure/functions");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  DynamoDBDocumentClient,
  ScanCommand,
  UpdateCommand,
} = require("@aws-sdk/lib-dynamodb");

const dynamoClient = new DynamoDBClient({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const dynamo = DynamoDBDocumentClient.from(dynamoClient);

app.timer("expireReceipts", {
  schedule: "0 0 0 * * *",

  handler: async (timer, context) => {
    context.log("Starting receipt expiration function...");

    const EXPENSES_TABLE = process.env.DYNAMODB_TABLE_EXPENSES;

    if (!EXPENSES_TABLE) {
      throw new Error("Missing DYNAMODB_TABLE_EXPENSES environment variable");
    }

    const now = new Date().toISOString();

    let lastEvaluatedKey = undefined;
    let checkedCount = 0;
    let expiredCount = 0;

    do {
      const scanResult = await dynamo.send(
        new ScanCommand({
          TableName: EXPENSES_TABLE,
          FilterExpression:
            "hasReceipt = :trueValue AND attribute_exists(receiptBlobName) AND attribute_exists(receiptExpiresAt) AND receiptExpiresAt <= :now",
          ExpressionAttributeValues: {
            ":trueValue": true,
            ":now": now,
          },
          ExclusiveStartKey: lastEvaluatedKey,
        })
      );

      const expiredExpenses = scanResult.Items || [];

      for (const expense of expiredExpenses) {
        checkedCount++;

        if (!expense.tripId || !expense.id) {
          context.log("Skipping expense without valid key:", expense);
          continue;
        }

        await dynamo.send(
          new UpdateCommand({
            TableName: EXPENSES_TABLE,
            Key: {
              tripId: expense.tripId,
              id: expense.id,
            },
            UpdateExpression:
              "SET hasReceipt = :falseValue, receiptBlobName = :nullValue, receiptOriginalName = :nullValue, receiptExpiresAt = :nullValue, receiptExpiredAt = :expiredAt",
            ExpressionAttributeValues: {
              ":falseValue": false,
              ":nullValue": null,
              ":expiredAt": now,
            },
          })
        );

        expiredCount++;

        context.log(
          `Expired receipt for expense ${expense.id} in trip ${expense.tripId}`
        );
      }

      lastEvaluatedKey = scanResult.LastEvaluatedKey;
    } while (lastEvaluatedKey);

    context.log("Receipt expiration function completed.");
    context.log(`Expired receipts updated: ${expiredCount}`);
  },
});