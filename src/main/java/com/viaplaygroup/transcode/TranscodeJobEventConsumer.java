package com.viaplaygroup.transcode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
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
import org.apache.commons.io.FilenameUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TranscodeJobEventConsumer extends AbstractVerticle {
    final static Logger logger = LoggerFactory.getLogger(TranscodeJobEventConsumer.class);

    private String mediaDirectory;

    private final static String QUEUE_NAME = "transcode";

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
            TranscodeDto transcodeDto =  objectMapper.readValue(message,TranscodeDto.class);
            /*
\"ffmpeg -y -i \"%s.mp4\" -an -c:v libx264 -x264opts 'keyint=24:min-keyint=24:no-scenecut' -b:v 1500k -vf \"scale=-2:720\" -f mp4 -pass 1 -y /dev/null
\"ffmpeg -y -i \"%s.mp4\" -an -c:v libx264 -x264opts 'keyint=24:min-keyint=24:no-scenecut' -b:v 1500k -vf \"scale=-2:720\" -f mp4 -pass 2 \"%s_1500.mp4\"
\"ffmpeg -y -i \"%s.mp4\" -an -c:v libx264 -x264opts 'keyint=24:min-keyint=24:no-scenecut' -b:v 800k -vf \"scale=-2:540\" -f mp4 -pass 1 -y /dev/null
\"ffmpeg -y -i \"%s.mp4\" -an -c:v libx264 -x264opts 'keyint=24:min-keyint=24:no-scenecut' -b:v 800k -vf \"scale=-2:540\" -f mp4 -pass 2 \"%s_800.mp4\"
\"ffmpeg -y -i \"%s.mp4\" -an -c:v libx264 -x264opts 'keyint=24:min-keyint=24:no-scenecut' -b:v 400k -vf \"scale=-2:360\" -f mp4 -pass 1 -y /dev/null
\"ffmpeg -y -i \"%s.mp4\" -an -c:v libx264 -x264opts 'keyint=24:min-keyint=24:no-scenecut' -b:v 400k -vf \"scale=-2:360\" -f mp4 -pass 2 \"%s_400.mp4\"
\"ffmpeg -y -i \"%s.mp4\" -an -c:v libx264 -x264opts 'keyint=24:min-keyint=24:no-scenecut' -b:v 200k -vf \"scale=-2:180\" -f mp4 -pass 1 -y /dev/null
\"ffmpeg -y -i \"%s.mp4\" -an -c:v libx264 -x264opts 'keyint=24:min-keyint=24:no-scenecut' -b:v 200k -vf \"scale=-2:180\" -f mp4 -pass 2 \"%s_200.mp4\"
rm -f ffmpeg*log*
MP4Box -dash 2000 -rap -frag-rap -profile onDemand \"%s_1500.mp4\" \"%s_800.mp4\" \"%s_400.mp4\" \"%s_200.mp4\" \"%s_audio.m4a\" -out \"%s_MP4.mpd\"
rm  \"%s_1500.mp4\" \"%s_800.mp4\" \"%s_400.mp4\" \"%s_200.mp4\" \"%s_audio.m4a\"

             */
            transcodeDto.sourceFileFullPath.matches(".+/([^/]+)");
            Path pathSourceFile = Paths.get(transcodeDto.sourceFileFullPath);
            String fileParts[] =pathSourceFile.getFileName().toString().split("\\.(?=[^\\.]+$)");
            String fileNameBase = fileParts[0];

            List<String> commands = Arrays.asList(
                String.format("ffmpeg -y -i \"%s.mp4\" -c:a  aac -ac 2 -ab 128k -vn \"%s_audio.m4a\"" ,fileNameBase ,fileNameBase),
                String.format("ffmpeg -y -i \"%s.mp4\" -an -c:v libx264 -x264opts 'keyint=24:min-keyint=24:no-scenecut' -b:v 1500k -vf \"scale=-2:720\" -f mp4 -pass 1 -y /dev/null",fileNameBase),
                String.format("ffmpeg -y -i \"%s.mp4\" -an -c:v libx264 -x264opts 'keyint=24:min-keyint=24:no-scenecut' -b:v 1500k -vf \"scale=-2:720\" -f mp4 -pass 2 \"%s_1500.mp4\"",fileNameBase, fileNameBase),
                String.format("ffmpeg -y -i \"%s.mp4\" -an -c:v libx264 -x264opts 'keyint=24:min-keyint=24:no-scenecut' -b:v 800k -vf \"scale=-2:540\" -f mp4 -pass 1 -y /dev/null",fileNameBase),
                String.format("ffmpeg -y -i \"%s.mp4\" -an -c:v libx264 -x264opts 'keyint=24:min-keyint=24:no-scenecut' -b:v 800k -vf \"scale=-2:540\" -f mp4 -pass 2 \"%s_800.mp4\"",fileNameBase, fileNameBase),
                String.format("ffmpeg -y -i \"%s.mp4\" -an -c:v libx264 -x264opts 'keyint=24:min-keyint=24:no-scenecut' -b:v 400k -vf \"scale=-2:360\" -f mp4 -pass 1 -y /dev/null",fileNameBase),
                String.format("ffmpeg -y -i \"%s.mp4\" -an -c:v libx264 -x264opts 'keyint=24:min-keyint=24:no-scenecut' -b:v 400k -vf \"scale=-2:360\" -f mp4 -pass 2 \"%s_400.mp4\"",fileNameBase, fileNameBase),
                String.format("ffmpeg -y -i \"%s.mp4\" -an -c:v libx264 -x264opts 'keyint=24:min-keyint=24:no-scenecut' -b:v 200k -vf \"scale=-2:180\" -f mp4 -pass 1 -y /dev/null",fileNameBase),
                String.format("ffmpeg -y -i \"%s.mp4\" -an -c:v libx264 -x264opts 'keyint=24:min-keyint=24:no-scenecut' -b:v 200k -vf \"scale=-2:180\" -f mp4 -pass 2 \"%s_200.mp4\"",fileNameBase, fileNameBase),
                String.format( "rm -f ffmpeg*log*"),
                String.format("MP4Box -dash 2000 -rap -frag-rap -profile onDemand \"%s_1500.mp4\" \"%s_800.mp4\" \"%s_400.mp4\" \"%s_200.mp4\" \"%s_audio.m4a\" -out \"%s_MP4.mpd\"",fileNameBase, fileNameBase,fileNameBase, fileNameBase,fileNameBase, fileNameBase),
                String.format("rm  \"%s_1500.mp4\" \"%s_800.mp4\" \"%s_400.mp4\" \"%s_200.mp4\" \"%s_audio.m4a\"",fileNameBase, fileNameBase,fileNameBase, fileNameBase,fileNameBase)
            );

            commands.stream().forEachOrdered( cmd -> {
                runCommand(transcodeDto.sourceDirectory, cmd);
            });

            try {
                HttpRequest requestGet = HttpRequest.newBuilder()
                        .uri(new URI("http://localhost:8080/api/media/"+transcodeDto.mediaId))
                        .GET()
                        .build();
                HttpClient client = HttpClient.newHttpClient();
                HttpResponse<String> response = client.send(requestGet, HttpResponse.BodyHandlers.ofString());
                MediaDto media = objectMapper.readValue(response.body(), MediaDto.class);

                MediaDto mediaMpd = new MediaDto();
                mediaMpd.assetId = media.assetId;
                mediaMpd.filePath = media.filePath;
                mediaMpd.fileName = String.format("%s_MP4.mpd", fileNameBase);

                HttpRequest requestPost = HttpRequest.newBuilder()
                        .uri(new URI("http://localhost:8080/api/media"))
                        .headers("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(mediaMpd)))
                        .build();
                HttpResponse<String> responsePost = client.send(requestPost, HttpResponse.BodyHandlers.ofString());


            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

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

