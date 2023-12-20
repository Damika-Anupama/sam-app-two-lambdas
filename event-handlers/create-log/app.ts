import { APIGatewayProxyHandler } from 'aws-lambda';
import { S3 } from 'aws-sdk';
import moment from 'moment';


const s3 = new S3();
const bucket = process.env.BUCKET_NAME; // get the bucket name from the environment variable

export const lambdaHandler: APIGatewayProxyHandler = async (event, context, callback) => {
  try {
    // parse the request body
    const body = JSON.parse(event.body);
    const group = body.group;
    const message = body.message;

    // validate the input
    if (!group || !message) {
      return callback(null, {
        statusCode: 400,
        body: 'group and message are required',
      });
    }

    // generate the log file path
    const date = moment().utc(); // get the current date and time in UTC
    const path = `${group}/${date.format('YYYY/MM/DD')}/logs_${date.format(
      'HH_mm'
    )}.log`; // format the path as /group/yyyy/MM/dd/logs_HH_mm.log

    // append the message to the log file
    await s3
      .upload({
        Bucket: bucket,
        Key: path,
        Body: message + '\n', // add a newline character at the end of the message
        ContentType: 'text/plain',
      })
      .promise();

    // return a success response
    return callback(null, {
      statusCode: 200,
      body: 'log created',
    });
  } catch (error) {
    // return an error response
    return callback(null, {
      statusCode: 500,
      body: error.message,
    });
  }
};
