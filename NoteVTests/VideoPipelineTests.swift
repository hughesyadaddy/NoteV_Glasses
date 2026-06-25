import AVFoundation
import XCTest
@testable import NoteV

final class VideoPipelineTests: XCTestCase {

    // MARK: - PTS Conversion

    func testSessionTimestampFromPresentationTime() {
        let start = CMTime(seconds: 10.0, preferredTimescale: 600)
        let pts = CMTime(seconds: 15.5, preferredTimescale: 600)

        let sessionTime = VisualSampleProcessor.sessionTimestamp(
            presentationTime: pts,
            sessionStart: start
        )

        XCTAssertEqual(sessionTime, 5.5, accuracy: 0.001)
    }

    func testEstablishTimebaseAlignsAudioAndVideo() {
        let processor = VisualSampleProcessor()
        let videoPTS = CMTime(seconds: 100.0, preferredTimescale: 600)
        let audioPTS = CMTime(seconds: 102.25, preferredTimescale: 600)

        let videoBuffer = makeSampleBuffer(presentationTime: videoPTS, mediaType: .video)
        let audioBuffer = makeSampleBuffer(presentationTime: audioPTS, mediaType: .audio)

        processor.processVideoSample(videoBuffer)
        let audioTimestamp = processor.establishTimebaseIfNeeded(for: audioBuffer)

        XCTAssertEqual(audioTimestamp, 2.25, accuracy: 0.001)
    }

    func testProcessorThrottlesPeriodicFrames() async {
        let processor = VisualSampleProcessor()
        var frames: [TimestampedFrame] = []

        let collectTask = Task {
            for await frame in processor.frameStream {
                frames.append(frame)
                if frames.count >= 2 { break }
            }
        }

        let timescale: CMTimeScale = 600
        processor.processVideoSample(makeVideoSampleBuffer(presentationTime: CMTime(seconds: 0, preferredTimescale: timescale)))
        processor.processVideoSample(makeVideoSampleBuffer(presentationTime: CMTime(seconds: 1, preferredTimescale: timescale)))
        processor.processVideoSample(makeVideoSampleBuffer(presentationTime: CMTime(seconds: 5, preferredTimescale: timescale)))

        try? await Task.sleep(nanoseconds: 300_000_000)
        collectTask.cancel()

        XCTAssertEqual(frames.count, 2)
        XCTAssertEqual(frames[0].timestamp, 0, accuracy: 0.01)
        XCTAssertEqual(frames[1].timestamp, 5, accuracy: 0.01)
        XCTAssertFalse(frames[0].imageData?.isEmpty ?? true)
    }

    func testProcessorBurstSamplingInterval() async {
        let processor = VisualSampleProcessor()
        var frames: [TimestampedFrame] = []

        let collectTask = Task {
            for await frame in processor.frameStream {
                frames.append(frame)
                if frames.count >= 3 { break }
            }
        }

        let timescale: CMTimeScale = 600
        processor.processVideoSample(makeVideoSampleBuffer(presentationTime: CMTime(seconds: 0, preferredTimescale: timescale)))
        processor.setSamplingInterval(1.0)
        try? await Task.sleep(nanoseconds: 50_000_000)
        processor.processVideoSample(makeVideoSampleBuffer(presentationTime: CMTime(seconds: 0.5, preferredTimescale: timescale)))
        processor.processVideoSample(makeVideoSampleBuffer(presentationTime: CMTime(seconds: 1.0, preferredTimescale: timescale)))
        processor.processVideoSample(makeVideoSampleBuffer(presentationTime: CMTime(seconds: 2.0, preferredTimescale: timescale)))

        try? await Task.sleep(nanoseconds: 300_000_000)
        collectTask.cancel()

        XCTAssertEqual(frames.count, 3)
        XCTAssertEqual(frames[0].timestamp, 0, accuracy: 0.01)
        XCTAssertEqual(frames[1].timestamp, 1.0, accuracy: 0.01)
        XCTAssertEqual(frames[2].timestamp, 2.0, accuracy: 0.01)
    }

    func testProcessorFlushAndWaitBeforeFinishRecording() async throws {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("notev-test-\(UUID().uuidString).mp4")

        let processor = VisualSampleProcessor()
        let recorder = VideoRecorder()
        try recorder.startRecording(to: tempURL)
        processor.videoRecorder = recorder

        let buffer = makeVideoSampleBuffer(presentationTime: CMTime(seconds: 0, preferredTimescale: 600))
        processor.processVideoSample(buffer)
        await processor.flushAndWait()

        let result = try await recorder.finishRecording()
        XCTAssertNotNil(result)
        XCTAssertTrue(FileManager.default.fileExists(atPath: tempURL.path))

        try? FileManager.default.removeItem(at: tempURL)
    }

    // MARK: - SessionMetadata videoFilename

    func testSessionDataCodableRoundTripWithVideoFilename() throws {
        let metadata = SessionMetadata(
            captureSource: .phone,
            title: "Video Session",
            videoFilename: NoteVConfig.Storage.sessionVideoFilename
        )

        let session = SessionData(metadata: metadata)

        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(session)

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(SessionData.self, from: data)

        XCTAssertEqual(decoded.metadata.videoFilename, NoteVConfig.Storage.sessionVideoFilename)
    }

    func testSessionDataDecodesWithoutVideoFilename() throws {
        let json = """
        {
          "metadata": {
            "sessionId": "A1B2C3D4-E5F6-7890-ABCD-EF1234567890",
            "startDate": "2026-01-01T12:00:00Z",
            "captureSource": "phone",
            "title": "Legacy Session",
            "durationSeconds": 60
          },
          "frames": [],
          "transcriptSegments": [],
          "bookmarks": []
        }
        """
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let session = try decoder.decode(SessionData.self, from: Data(json.utf8))
        XCTAssertNil(session.metadata.videoFilename)
    }

    func testEnsureSessionDirectoryBeforeRecording() throws {
        let store = SessionStore()
        let sessionId = UUID()
        let sessionDir = store.sessionDirectory(for: sessionId)

        try? FileManager.default.removeItem(at: sessionDir)

        try store.ensureSessionDirectory(for: sessionId)
        XCTAssertTrue(FileManager.default.fileExists(atPath: sessionDir.path))

        try? FileManager.default.removeItem(at: sessionDir)
    }

    // MARK: - VideoRecorder lifecycle

    func testVideoRecorderFinishWithoutFramesReturnsNil() async throws {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("notev-test-\(UUID().uuidString).mp4")

        let recorder = VideoRecorder()
        try recorder.startRecording(to: tempURL)

        let result = try await recorder.finishRecording()
        XCTAssertNil(result)
        XCTAssertFalse(FileManager.default.fileExists(atPath: tempURL.path))
    }

    func testVideoRecorderRejectsDoubleStart() async throws {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("notev-test-\(UUID().uuidString).mp4")

        let recorder = VideoRecorder()
        try recorder.startRecording(to: tempURL)

        do {
            try recorder.startRecording(to: tempURL)
            XCTFail("Expected alreadyRecording error")
        } catch let error as VideoRecorder.VideoRecorderError {
            XCTAssertEqual(error.errorDescription, "Video recording already in progress")
        }

        _ = try await recorder.finishRecording()
        try? FileManager.default.removeItem(at: tempURL)
    }

    func testVideoRecorderWritesVideoFrame() async throws {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("notev-test-\(UUID().uuidString).mp4")

        let recorder = VideoRecorder()
        try recorder.startRecording(to: tempURL)

        let buffer = makeVideoSampleBuffer(presentationTime: CMTime(seconds: 0, preferredTimescale: 600))
        recorder.appendVideo(buffer)

        // Allow async append on recorder queue.
        try await Task.sleep(nanoseconds: 200_000_000)

        let result = try await recorder.finishRecording()
        XCTAssertNotNil(result)
        XCTAssertTrue(FileManager.default.fileExists(atPath: tempURL.path))

        try? FileManager.default.removeItem(at: tempURL)
    }

    func testVideoRecorderMuxesAudioTrack() async throws {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("notev-test-\(UUID().uuidString).mp4")

        let recorder = VideoRecorder()
        try recorder.startRecording(to: tempURL)

        let videoBuffer = makeVideoSampleBuffer(presentationTime: CMTime(seconds: 0, preferredTimescale: 600))
        recorder.appendVideo(videoBuffer)

        let pcm = Data(repeating: 0, count: 3200)
        guard let audioBuffer = AudioSampleBufferFactory.makePCMSampleBuffer(
            data: pcm,
            sampleRate: Double(NoteVConfig.Audio.muxSampleRate),
            channels: 1,
            presentationTime: CMTime(seconds: 0, preferredTimescale: 600)
        ) else {
            XCTFail("Could not build audio sample buffer")
            return
        }
        recorder.appendAudio(audioBuffer)

        try await Task.sleep(nanoseconds: 300_000_000)

        let result = try await recorder.finishRecording()
        XCTAssertNotNil(result)

        let asset = AVURLAsset(url: tempURL)
        let audioTracks = try await asset.loadTracks(withMediaType: .audio)
        XCTAssertEqual(audioTracks.count, 1)

        try? FileManager.default.removeItem(at: tempURL)
    }

    func testVideoRecorderPreservesAudioBufferedBeforeFirstVideo() async throws {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("notev-test-\(UUID().uuidString).mp4")

        let recorder = VideoRecorder()
        try recorder.startRecording(to: tempURL)

        let pcm = Data(repeating: 0, count: 3200)
        guard let audioBuffer = AudioSampleBufferFactory.makePCMSampleBuffer(
            data: pcm,
            sampleRate: Double(NoteVConfig.Audio.muxSampleRate),
            channels: 1,
            presentationTime: CMTime(seconds: 0, preferredTimescale: 600)
        ) else {
            XCTFail("Could not build audio sample buffer")
            return
        }
        recorder.appendAudio(audioBuffer)

        try await Task.sleep(nanoseconds: 300_000_000)

        let videoBuffer = makeVideoSampleBuffer(presentationTime: CMTime(seconds: 0.1, preferredTimescale: 600))
        recorder.appendVideo(videoBuffer)

        try await Task.sleep(nanoseconds: 300_000_000)

        XCTAssertGreaterThan(recorder.muxedAudioSampleCount, 0)

        let result = try await recorder.finishRecording()
        XCTAssertNotNil(result)

        let asset = AVURLAsset(url: tempURL)
        let audioTracks = try await asset.loadTracks(withMediaType: .audio)
        let videoTracks = try await asset.loadTracks(withMediaType: .video)
        XCTAssertEqual(audioTracks.count, 1)
        XCTAssertEqual(videoTracks.count, 1)

        try? FileManager.default.removeItem(at: tempURL)
    }

    func testProcessorMuxesGlassesPCMOnVideoTimeline() async throws {
        let tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("notev-test-\(UUID().uuidString).mp4")

        let processor = VisualSampleProcessor()
        let recorder = VideoRecorder()
        try recorder.startRecording(to: tempURL)
        processor.videoRecorder = recorder

        // Simulate glasses audio arriving on wall clock before/after video (misaligned timestamps).
        let pcm = Data(repeating: 0, count: 3200)
        processor.processAudioPCM(data: pcm, sessionRelativeTime: 12.0, sampleRate: Double(NoteVConfig.Audio.muxSampleRate))

        let videoBuffer = makeVideoSampleBuffer(presentationTime: CMTime(seconds: 0, preferredTimescale: 600))
        processor.processVideoSample(videoBuffer)

        processor.processAudioPCM(data: pcm, sessionRelativeTime: 20.0, sampleRate: Double(NoteVConfig.Audio.muxSampleRate))

        await processor.flushAndWait()

        XCTAssertGreaterThan(recorder.muxedAudioSampleCount, 0)

        let result = try await recorder.finishRecording()
        XCTAssertNotNil(result)

        let asset = AVURLAsset(url: tempURL)
        let audioTracks = try await asset.loadTracks(withMediaType: .audio)
        XCTAssertEqual(audioTracks.count, 1)

        try? FileManager.default.removeItem(at: tempURL)
    }

    // MARK: - SessionStore

    func testSessionStoreVideoURL() {
        let store = SessionStore()
        let sessionId = UUID()
        let url = store.videoURL(for: sessionId)

        XCTAssertTrue(url.lastPathComponent == NoteVConfig.Storage.sessionVideoFilename)
        XCTAssertTrue(url.path.contains(sessionId.uuidString))
    }

    // MARK: - Helpers

    private func makeVideoSampleBuffer(presentationTime: CMTime) -> CMSampleBuffer {
        var pixelBuffer: CVPixelBuffer?
        let attrs: [String: Any] = [
            kCVPixelBufferCGImageCompatibilityKey as String: true,
            kCVPixelBufferCGBitmapContextCompatibilityKey as String: true
        ]
        CVPixelBufferCreate(
            kCFAllocatorDefault,
            64,
            64,
            kCVPixelFormatType_32BGRA,
            attrs as CFDictionary,
            &pixelBuffer
        )

        guard let pixelBuffer else {
            fatalError("Failed to create pixel buffer")
        }

        CVPixelBufferLockBaseAddress(pixelBuffer, [])
        if let base = CVPixelBufferGetBaseAddress(pixelBuffer) {
            memset(base, 0xFF, CVPixelBufferGetDataSize(pixelBuffer))
        }
        CVPixelBufferUnlockBaseAddress(pixelBuffer, [])

        var formatDescription: CMFormatDescription?
        CMVideoFormatDescriptionCreateForImageBuffer(
            allocator: kCFAllocatorDefault,
            imageBuffer: pixelBuffer,
            formatDescriptionOut: &formatDescription
        )

        guard let formatDescription else {
            fatalError("Failed to create format description")
        }

        var timing = CMSampleTimingInfo(
            duration: CMTime(value: 1, timescale: 30),
            presentationTimeStamp: presentationTime,
            decodeTimeStamp: .invalid
        )

        var sampleBuffer: CMSampleBuffer?
        CMSampleBufferCreateForImageBuffer(
            allocator: kCFAllocatorDefault,
            imageBuffer: pixelBuffer,
            dataReady: true,
            makeDataReadyCallback: nil,
            refcon: nil,
            formatDescription: formatDescription,
            sampleTiming: &timing,
            sampleBufferOut: &sampleBuffer
        )

        guard let sampleBuffer else {
            fatalError("Failed to create sample buffer")
        }
        return sampleBuffer
    }

    private func makeSampleBuffer(presentationTime: CMTime, mediaType: AVMediaType) -> CMSampleBuffer {
        if mediaType == .video {
            return makeVideoSampleBuffer(presentationTime: presentationTime)
        }

        var formatDescription: CMFormatDescription?
        var asbd = AudioStreamBasicDescription(
            mSampleRate: 44100,
            mFormatID: kAudioFormatLinearPCM,
            mFormatFlags: kAudioFormatFlagIsSignedInteger | kAudioFormatFlagIsPacked,
            mBytesPerPacket: 2,
            mFramesPerPacket: 1,
            mBytesPerFrame: 2,
            mChannelsPerFrame: 1,
            mBitsPerChannel: 16,
            mReserved: 0
        )
        CMAudioFormatDescriptionCreate(
            allocator: kCFAllocatorDefault,
            asbd: &asbd,
            layoutSize: 0,
            layout: nil,
            magicCookieSize: 0,
            magicCookie: nil,
            extensions: nil,
            formatDescriptionOut: &formatDescription
        )

        guard let formatDescription else {
            fatalError("Failed to create format description")
        }

        var timing = CMSampleTimingInfo(
            duration: CMTime(value: 1, timescale: 30),
            presentationTimeStamp: presentationTime,
            decodeTimeStamp: .invalid
        )

        var sampleBuffer: CMSampleBuffer?
        CMSampleBufferCreate(
            allocator: kCFAllocatorDefault,
            dataBuffer: nil,
            dataReady: true,
            makeDataReadyCallback: nil,
            refcon: nil,
            formatDescription: formatDescription,
            sampleCount: 1,
            sampleTimingEntryCount: 1,
            sampleTimingArray: &timing,
            sampleSizeEntryCount: 0,
            sampleSizeArray: nil,
            sampleBufferOut: &sampleBuffer
        )

        guard let sampleBuffer else {
            fatalError("Failed to create sample buffer")
        }
        return sampleBuffer
    }
}
