# VideoTranscoder

This repository contains the code and setup instructions for a scalable video processing pipeline. The pipeline is designed to handle video uploads, validation, and processing using AWS services and containerized applications.
![image](https://github.com/user-attachments/assets/7c87809e-9ac4-469b-84bb-2e174933f5c2)


## Overview
The pipeline consists of the following steps:

1. **Temporary Storage (S3 Bucket - Temp)**
   - Users upload videos to a temporary S3 bucket.

2. **Message Queue (SQS)**
   - Once a video is uploaded, an event is triggered that sends a message to an Amazon Simple Queue Service (SQS) queue.
   - The SQS queue acts as a buffer to handle incoming video processing requests.

3. **Processing Workers (Node.js)**
   - A Node.js workers constantly poll the SQS queue for new messages.
   - The worker is responsible for downloading the video, validating the file, and sending it to the processing container.

4. **Processing Container (Docker - ffmpeg)**
   - The validated video file is then downloaded into a Docker container running `ffmpeg`.
   - The video is processed (e.g., encoding, transcoding, or other manipulations).

5. **Production Storage (S3 Bucket - Production)**
   - After processing, the video is uploaded to a production S3 bucket for final storage and distribution.

## Prerequisites

- AWS account with access to S3 and SQS services.
- Docker installed on the machine where the processing will occur.
- Node.js installed to run the workers.

## Setup Instructions

1. **AWS S3 Buckets**
   - Create two S3 buckets: one for temporary storage (`Temp`) and one for production (`Production`).
   - Set appropriate permissions for these buckets to allow read and write, cors operations by the processing service.

2. **AWS SQS**
   - Set up an SQS queue that will handle messages triggered by new video uploads.

3. **Node.js Workers**
   - Install the necessary dependencies by running `npm install`.
   - Configure the workers with the necessary AWS credentials and S3 bucket information.

4. **Docker Setup**
   - Build and run the Docker container with `ffmpeg` using the provided `Dockerfile`.
   - Ensure the container is configured to receive video files from the workers for processing.

5. **Environment Variables**
   - Configure environment variables such as S3 bucket names, SQS queue URLs, and AWS credentials.

## Running the Pipeline

1. Start the Node.js worker in video-consumer to poll the SQS queue:
   ```bash
   pnpm dev
   ```
2. Ensure the Docker container is running and ready to process the video files.