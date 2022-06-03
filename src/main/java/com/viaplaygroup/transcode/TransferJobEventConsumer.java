package com.viaplaygroup.transcode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import io.minio.errors.MinioException;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

public class TransferJobEventConsumer extends AbstractVerticle {
    final static Logger logger = LoggerFactory.getLogger(TransferJobEventConsumer.class);

    private String mediaDirectory;

    private final static String QUEUE_NAME = "transfer";

    @Override
    public void start() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" + message + "'");
            ObjectMapper objectMapper = new ObjectMapper();
            TransferDto transferDto =  objectMapper.readValue(message,TransferDto.class);


            try {
                // Create a minioClient with the MinIO server playground, its access key and secret key.
                MinioClient minioClient =
                        MinioClient.builder()
                                .endpoint("https://play.min.io")
                                .credentials("Q3AM3UQ867SPQQA43P2F", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG")
                                .build();

                // Make 'asiatrip' bucket if not exist.
                boolean found =
                        minioClient.bucketExists(BucketExistsArgs.builder().bucket("minhajtest").build());
                if (!found) {
                    // Make a new bucket called 'asiatrip'.
                    minioClient.makeBucket(MakeBucketArgs.builder().bucket("minhajtest").build());
                } else {
                    System.out.println("Bucket 'minhajtest' already exists.");
                }

                // Upload '/home/user/Photos/asiaphotos.zip' as object name 'asiaphotos-2015.zip' to bucket
                // 'asiatrip'.
                File file = new File(transferDto.sourceFileFullPath);
                minioClient.uploadObject(
                        UploadObjectArgs.builder()
                                .bucket("minhajtest")
                                .object(file.getName())
                                .filename(transferDto.sourceFileFullPath)
                                .build());
                System.out.println(
                        "'' is successfully uploaded as "
                                + "object '' to bucket 'minhajtest'.");
            } catch (MinioException | InvalidKeyException | NoSuchAlgorithmException e) {
                System.out.println("Error occurred: " + e);
//                System.out.println("HTTP trace: " + e.httpTrace());
            }
//            try {
//                HttpRequest requestGet = HttpRequest.newBuilder()
//                        .uri(new URI("http://localhost:8080/api/media/"+transferDto.mediaId))
//                        .GET()
//                        .build();
//                HttpClient client = HttpClient.newHttpClient();
//                HttpResponse<String> response = client.send(requestGet, HttpResponse.BodyHandlers.ofString());
//                MediaDto media = objectMapper.readValue(response.body(), MediaDto.class);
//
//                MediaDto mediaMpd = new MediaDto();
//                mediaMpd.assetId = media.assetId;
//                mediaMpd.filePath = media.filePath;
//                mediaMpd.fileName = String.format("%s_MP4.mpd", fileNameBase);
//
//                HttpRequest requestPost = HttpRequest.newBuilder()
//                        .uri(new URI("http://localhost:8080/api/media"))
//                        .headers("Content-Type", "application/json")
//                        .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(mediaMpd)))
//                        .build();
//                HttpResponse<String> responsePost = client.send(requestPost, HttpResponse.BodyHandlers.ofString());
//
//
//            } catch (URISyntaxException e) {
//                throw new RuntimeException(e);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }

        };
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });


//        this.mediaDirectory = System.getenv("mediadirectory");
//        vertx.eventBus().<TranscodeJobDTO>consumer("transcodejob", transcodeJobEvent -> {
//            logger.info("Event received: ================================================== "+transcodeJobEvent.body());
//            WorkerExecutor executor = vertx.createSharedWorkerExecutor("transcode-job-worker-pool");
//            executor.executeBlocking(promise -> {
//                TranscodeJobDTO transcodeJob = transcodeJobEvent.body();
//                FFmpeg ffmpeg = null;
//                FFprobe ffprobe = null;
//                try {
//                    ffmpeg = new FFmpeg("/usr/bin/ffmpeg");
//                    ffprobe = new FFprobe("/usr/bin/ffprobe");
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                String fullFilePath = mediaDirectory + transcodeJob.getFileId() + "/" + transcodeJob.getFileName();
//                logger.info("Media file path: "+fullFilePath);
//                FFmpegBuilder builder = new FFmpegBuilder()
//                    .setInput(fullFilePath)     // Filename, or a FFmpegProbeResult
//                    .overrideOutputFiles(true) // Override the output if it exists
//                    .addOutput(mediaDirectory + transcodeJob.getFileId() + "/" + transcodeJob.getOutputFileName())   // Filename for the destination
//                    .setFormat(transcodeJob.getTargetFormat())        // Format is inferred from filename, or can be set
//                    // .setTargetSize(250_000)  // Aim for a 250KB file
//                    .disableSubtitle()       // No subtiles
//                    .setAudioChannels(1)         // Mono audio
//                    .setAudioCodec("aac")        // using the aac codec
//                    .setAudioSampleRate(48_000)  // at 48KHz
//                    .setAudioBitRate(32768)      // at 32 kbit/s
//                    .setVideoCodec("libx264")     // Video using x264
//                    .setVideoFrameRate(24, 1)     // at 24 frames per second
//                    .setVideoResolution(640, 480) // at 640x480 resolution
//                    .setStrict(FFmpegBuilder.Strict.EXPERIMENTAL) // Allow FFmpeg to use experimental specs
//                    .done();
//                FFmpegExecutor ffmpegExecutor = new FFmpegExecutor(ffmpeg, ffprobe);
//                FFmpegProbeResult in = null;
//                try {
//                    in = ffprobe.probe(fullFilePath);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                final FFmpegProbeResult fin = in;
//
//                FFmpegJob ffmpegJob = ffmpegExecutor.createJob(builder, new ProgressListener() {
//                    final double duration_ns = fin.getFormat().duration * TimeUnit.SECONDS.toNanos(1);
//
//                    @Override
//                    public void progress(Progress progress) {
//                        double percentage = progress.out_time_ns / duration_ns;
//
//                        // Print out interesting information about the progress
//                        logger.info(String.format(
//                            "[%.0f%%] status:%s frame:%d time:%s ms fps:%.0f speed:%.2fx",
//                            percentage * 100,
//                            progress.status,
//                            progress.frame,
//                            FFmpegUtils.toTimecode(progress.out_time_ns, TimeUnit.NANOSECONDS),
//                            progress.fps.doubleValue(),
//                            progress.speed
//                        ));
//                    }
//                });
//                ffmpegJob.run();
//                // Or run a two-pass encode (which is better quality at the cost of being slower)
//                // executor.createTwoPassJob(builder).run();
//                promise.complete("Done");
//            }, asyncResult -> {
//                logger.info("The result is: " + asyncResult.result());
//            });
//            transcodeJobEvent.reply(transcodeJobEvent.body());
//        });
    }

    public static void runCommand(String workingDirectory, String command) {
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.directory(new File(workingDirectory));
        processBuilder.command("/bin/bash", "-c", command);
        System.out.println("Running cmd: "+command);
        try {
            Process process = processBuilder.start();
            StringBuilder output = new StringBuilder();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
            int exitVal = process.waitFor();
            if (exitVal != 0) {
                System.out.println("Exit value is non-zero: "+exitVal);
                System.out.println(output);
            } else {
                System.out.println("Success");
                System.out.println(output);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }
}

