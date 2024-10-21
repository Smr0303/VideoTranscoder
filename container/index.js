require('dotenv').config();

const { S3Client, GetObjectCommand, PutObjectCommand } = require("@aws-sdk/client-s3");
const fs = require("node:fs/promises");
const fsOld = require("node:fs");
const path = require("node:path");
const ffmpeg = require("fluent-ffmpeg");

const RESOLUTIONS = [
    { name: "360p", width: 480, height: 360 },
    { name: "480p", width: 858, height: 480 },
    { name: "720p", width: 1280, height: 720 }
];

const s3Client = new S3Client({
    region: "us-east-1",
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
});

const BUCKET_NAME = process.env.BUCKET_NAME;
const KEY = process.env.KEY;

async function init() {
    try {
        const command = new GetObjectCommand({
            Bucket: BUCKET_NAME,
            Key: KEY,
        });

        const result = await s3Client.send(command);
        const originalFilePath = 'original-video.mp4';
        await fs.writeFile(originalFilePath, result.Body);

        const originalVideoPath = path.resolve(originalFilePath);

        // Start the transcoder for each resolution
        const promises = RESOLUTIONS.map(resolution => {
            return new Promise((resolve) => {
                const output = `video-${resolution.name}.mp4`;

                ffmpeg(originalVideoPath)
                    .output(output)
                    .withVideoCodec("libx264")
                    .withAudioCodec("aac")
                    .withSize(`${resolution.width}x${resolution.height}`)
                    .on('end', async () => {
                        const putCommand = new PutObjectCommand({
                            Bucket: BUCKET_NAME,
                            Key: output,
                            Body: fsOld.createReadStream(path.resolve(output)),
                        });

                        await s3Client.send(putCommand);
                        resolve();
                        console.log(`Uploaded ${output}`);
                    })
                    .format("mp4")
                    .run();
            });
        });

        await Promise.all(promises);
        console.log("All videos have been transcoded and uploaded successfully.");
    } catch (error) {
        console.error("Error in video processing:", error);
    }
}

init();
