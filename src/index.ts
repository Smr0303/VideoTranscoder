import { config } from 'dotenv';
config();

import { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } from "@aws-sdk/client-sqs";
import { ECSClient, RunTaskCommand } from "@aws-sdk/client-ecs";
import type { S3Event } from "aws-lambda";

// Initialize SQS and ECS clients with environment variables
const client = new SQSClient({
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
    },
});

const ecsClient = new ECSClient({
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
    },
});

async function init() {
    const command = new ReceiveMessageCommand({
        QueueUrl: process.env.SQS_QUEUE_URL,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 10,
    });

    while (true) {
        const { Messages } = await client.send(command);
        if (!Messages) {
            console.log("No messages received");
            continue;
        }

        try {
            for (const message of Messages) {
                const { MessageId, Body } = message;
                console.log("Message received", { MessageId, Body });

                if (!Body) continue;

                const event = JSON.parse(Body) as S3Event;

                // Ignore test events
                if ("Service" in event && "Event" in event && event.Event === "s3:TestEvent") {
                    await client.send(
                        new DeleteMessageCommand({
                            QueueUrl: process.env.SQS_QUEUE_URL!,
                            ReceiptHandle: message.ReceiptHandle,
                        })
                    );
                    continue;
                }

                for (const record of event.Records) {
                    const { eventName, s3 } = record;
                    const { bucket, object: { key } } = s3;

                    // Spin up the ECS task
                    const runTaskCommand = new RunTaskCommand({
                        taskDefinition: process.env.TASK_DEFINITION!,
                        cluster: process.env.CLUSTER!,
                        launchType: "FARGATE",
                        networkConfiguration: {
                            awsvpcConfiguration: {
                                assignPublicIp: "ENABLED",
                                securityGroups: process.env.SECURITY_GROUPS!.split(','),
                                subnets: process.env.SUBNETS!.split(','),
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

                    await ecsClient.send(runTaskCommand);
                    console.log(`ECS task started for video: ${key}`);

                    // Delete the message from the queue
                    await client.send(
                        new DeleteMessageCommand({
                            QueueUrl: process.env.SQS_QUEUE_URL!,
                            ReceiptHandle: message.ReceiptHandle,
                        })
                    );
                    console.log(`Message ${MessageId} deleted from the queue`);
                }
            }
        } catch (err) {
            console.error("Error processing message:", err);
        }
    }
}

init().catch((err) => console.error("Error initializing:", err));
