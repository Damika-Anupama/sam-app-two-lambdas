import { APIGatewayProxyHandler } from 'aws-lambda';
import { S3 } from 'aws-sdk';
import moment from 'moment';

const s3 = new S3();
const bucket = process.env.BUCKET_NAME; // get the bucket name from the environment variable
const maxLines = 1000; // set the maximum number of log lines to return

export const lambdaHandler: APIGatewayProxyHandler = async (event, context, callback) => {
  try {
    // parse the request parameters
    const group = event.pathParameters.group;
    const date = event.queryStringParameters.date;
    const from = event.queryStringParameters.from;
    const to = event.queryStringParameters.to;
    const filters = event.queryStringParameters.filters;

    // validate the input
    if (!group || !date) {
      return callback(null, {
        statusCode: 400,
        body: 'group and date are required',
      });
    }

    // parse the date and time parameters
    const dateMoment = moment(date, 'YYYYMMDD').utc(); // parse the date as yyyyMMdd and convert to UTC
    const fromMoment = from
      ? dateMoment.clone().add(moment.duration(from, 'HHmm')) // parse the from time as HHmm and add to the date
      : dateMoment.clone().startOf('day'); // use the start of the day as the default from time
    const toMoment = to
      ? dateMoment.clone().add(moment.duration(to, 'HHmm')) // parse the to time as HHmm and add to the date
      : dateMoment.clone().endOf('day'); // use the end of the day as the default to time

    // validate the date and time range
    if (!dateMoment.isValid() || !fromMoment.isValid() || !toMoment.isValid()) {
      return callback(null, {
        statusCode: 400,
        body: 'invalid date or time format',
      });
    }

    if (fromMoment.isAfter(toMoment)) {
      return callback(null, {
        statusCode: 400,
        body: 'from time must be before or equal to to time',
      });
    }

    // parse the filters parameter
    const filterArray = filters ? filters.split(',') : []; // split the filters by comma if present

    // generate the log file prefix
    const prefix = `${group}/${dateMoment.format('YYYY/MM/DD')}/logs_`; // format the prefix as /group/yyyy/MM/dd/logs_

    // list the log files that match the prefix and the time range
    const listResponse = await s3
      .listObjectsV2({
        Bucket: bucket,
        Prefix: prefix,
      })
      .promise();

    const logFiles = listResponse.Contents.filter((object) => {
      // extract the hour and minute from the object key
      const match = object.Key.match(/logs_(\d\d)_(\d\d).log$/);
      if (match) {
        const hour = parseInt(match[1]);
        const minute = parseInt(match[2]);
        // create a moment object with the date, hour, and minute
        const objectMoment = dateMoment.clone().set({ hour, minute });
        // check if the object moment is within the time range
        return objectMoment.isBetween(fromMoment, toMoment, 'minute', '[]');
      } else {
        return false;
      }
    });

    // sort the log files by key in ascending order
    logFiles.sort((a, b) => (a.Key < b.Key ? -1 : 1));

    // initialize an array to store the log lines
    const logLines = [];

    // loop through the log files and read the contents
    for (const logFile of logFiles) {
      // get the object from s3
      const objectResponse = await s3
        .getObject({
          Bucket: bucket,
          Key: logFile.Key,
        })
        .promise();

      // convert the object body to a string
      const objectBody = objectResponse.Body.toString();

      // split the object body by newline characters
      const objectLines = objectBody.split('\n');

      // loop through the object lines and filter them
      for (const objectLine of objectLines) {
        // check if the line is not empty
        if (objectLine) {
          // check if the line matches all the filters
          const match = filterArray.every((filter) =>
            objectLine.includes(filter)
          );
          // if the line matches, push it to the log lines array
          if (match) {
            logLines.push(objectLine);
          }
        }
      }

      // check if the log lines array has reached the maximum size
      if (logLines.length >= maxLines) {
        // break the loop
        break;
      }
    }

    // return a success response with the log lines as JSON
    return callback(null, {
      statusCode: 200,
      body: JSON.stringify(logLines),
    });
  } catch (error) {
    // return an error response
    return callback(null, {
      statusCode: 500,
      body: error.message,
    });
  }
};
