import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
    DynamoDBDocumentClient,
    QueryCommand,
    QueryCommandInput
} from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDocumentClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
    try {

        // Console.log the event to CloudWatch
        console.log("Event: ", JSON.stringify(event));
        
        // Parse out the parameters (according to event structure)
        const parameters = event?.pathParameters;
        const movieId = parameters?.movieId ? parseInt(parameters.movieId) : undefined;
        const awardBody = parameters?.awardBody;

        // Check for existence of Query String (part B)
        const queryString = event.queryStringParameters || {};
        // If Query String min value exists, parse its value as int. Else, leave min undefined
        const minAwards = queryString.min ? parseInt(queryString.min) : undefined;


        // If either the partition key or sort key are missing, handle this
        // Unlikely to occur since how would we send the request to
        // www.api.com/dev/awards//movies// ?
        if (!movieId || !awardBody) {
            return {
                statusCode: 404,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({ Message: "Missing movieId or awardBody" }),
            };
        }

        // Build the query command input
        let commandInput: QueryCommandInput = {
            TableName: process.env.AWARDS_TABLE_NAME,
            KeyConditionExpression: "movieId = :movieId AND awardBody = :awardBody",
            ExpressionAttributeValues: {
                ":movieId": movieId,
                ":awardBody": awardBody
            }
        }

        // Send the query command input
        const getCommandOutput = await ddbDocClient.send(new QueryCommand(commandInput));


        // If the response from the database is empty (i.e. no match was found)
        if (!getCommandOutput.Items || getCommandOutput.Items.length == 0) {
            return {
                statusCode: 404,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({ Message: "No award details found for the given movieId and awardBody" }),
            };
        }

        // This code was used to handle the event of multiple awards for the same movie
        // i.e. MovieID 1234 with Award: Oscars
        // 2 different award objects are returned
        // Filter the returned items and match on the criteria --> return item.numAwards > minAwards
        const filteredItems = getCommandOutput.Items.filter((item) => {
            if (minAwards != undefined) {
                return item.numAwards > minAwards
            }
            return true
        })

        // If the length == 0, that means that an item was returned from the database but failed the above filter (num awards lower than min awards)
        if (filteredItems.length == 0) {
            return {
                statusCode: 400,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({ Message: "Request failed (Award for movie found, but min value is greater than or equal to numAwards)" }),
            };
        }

        const body = {
            data: filteredItems,
        };

        // Return Response
        return {
            statusCode: 200,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify(body),
        };
    } catch (error: any) {
        console.error(JSON.stringify(error));
        return {
            statusCode: 500,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify({ error }),
        };
    }
};

function createDocumentClient() {
    const ddbClient = new DynamoDBClient({ region: process.env.REGION });
    const marshallOptions = {
        convertEmptyValues: true,
        removeUndefinedValues: true,
        convertClassInstanceToMap: true,
    };
    const unmarshallOptions = {
        wrapNumbers: false,
    };
    const translateConfig = { marshallOptions, unmarshallOptions };
    return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}