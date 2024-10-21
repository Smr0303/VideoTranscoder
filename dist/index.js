"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const dotenv_1 = require("dotenv");
(0, dotenv_1.config)();
const client_sqs_1 = require("@aws-sdk/client-sqs");
const client_ecs_1 = require("@aws-sdk/client-ecs");
// Initialize SQS and ECS clients with environment variables
const client = new client_sqs_1.SQSClient({
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
});
const ecsClient = new client_ecs_1.ECSClient({
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
});
function init() {
    return __awaiter(this, void 0, void 0, function* () {
        const command = new client_sqs_1.ReceiveMessageCommand({
            QueueUrl: process.env.SQS_QUEUE_URL,
            MaxNumberOfMessages: 1,
            WaitTimeSeconds: 10,
        });
        while (true) {
            const { Messages } = yield client.send(command);
            if (!Messages) {
                console.log("No messages received");
                continue;
            }
            try {
                for (const message of Messages) {
                    const { MessageId, Body } = message;
                    console.log("Message received", { MessageId, Body });
                    if (!Body)
                        continue;
                    const event = JSON.parse(Body);
                    // Ignore test events
                    if ("Service" in event && "Event" in event && event.Event === "s3:TestEvent") {
                        yield client.send(new client_sqs_1.DeleteMessageCommand({
                            QueueUrl: process.env.SQS_QUEUE_URL,
                            ReceiptHandle: message.ReceiptHandle,
                        }));
                        continue;
                    }
                    for (const record of event.Records) {
                        const { eventName, s3 } = record;
                        const { bucket, object: { key } } = s3;
                        // Spin up the ECS task
                        const runTaskCommand = new client_ecs_1.RunTaskCommand({
                            taskDefinition: process.env.TASK_DEFINITION,
                            cluster: process.env.CLUSTER,
                            launchType: "FARGATE",
                            networkConfiguration: {
                                awsvpcConfiguration: {
                                    assignPublicIp: "ENABLED",
                                    securityGroups: process.env.SECURITY_GROUPS.split(','),
                                    subnets: process.env.SUBNETS.split(','),
                                },
                            },
                            overrides: {
                                containerOverrides: [
                                    {
                                        name: "video-transcoder",
                                        environment: [
                                            { name: "BUCKET_NAME", value: bucket.name },
                                            { name: "KEY", value: key },
                                        ],
                                    },
                                ],
                            },
                        });
                        yield ecsClient.send(runTaskCommand);
                        console.log(`ECS task started for video: ${key}`);
                        // Delete the message from the queue
                        yield client.send(new client_sqs_1.DeleteMessageCommand({
                            QueueUrl: process.env.SQS_QUEUE_URL,
                            ReceiptHandle: message.ReceiptHandle,
                        }));
                        console.log(`Message ${MessageId} deleted from the queue`);
                    }
                }
            }
            catch (err) {
                console.error("Error processing message:", err);
            }
        }
    });
}
init().catch((err) => console.error("Error initializing:", err));
