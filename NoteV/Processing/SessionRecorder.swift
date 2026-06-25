import Foundation
import UIKit
import AVFoundation

// MARK: - SessionRecorder

/// Orchestrates all pipelines during a recording session.
/// Ties together CaptureManager, AudioPipeline, FramePipeline, and BookmarkDetector.
/// Forks transcript stream to both UI and BookmarkDetector.
/// Assembles the final SessionData when recording ends.
///
/// All pipeline instances are recreated per session to avoid stale AsyncStream issues.
@MainActor
final class SessionRecorder: ObservableObject {

    // MARK: - Properties

    /// Recreated per session — lazy AsyncStream vars are one-time-use
    private var captureManager: CaptureManager!
    private var audioPipeline: AudioPipeline!
    private var framePipeline: FramePipeline!
    // Voice bookmark disabled — keyword detection has reliability issues (interim vs final segments).
    // Manual bookmark via UI button still works.
    // private var bookmarkDetector: BookmarkDetector!

    private let imageStore = ImageStore()
    private let sessionStore = SessionStore()

    private weak var appState: AppState?
    private weak var sharedCaptureManager: CaptureManager?

    @Published private(set) var isRecording = false
    private var sessionStartTime: Date?
    private var sessionId: UUID?

    // Collected data
    private var collectedFrames: [TimestampedFrame] = []
    private var collectedSegments: [TranscriptSegment] = []
    private var collectedBookmarks: [Bookmark] = []

    // Smart bookmark detection
    private let smartDetector = SmartBookmarkDetector()
    private var textBuffer: String = ""
    private var autoBookmarkCount = 0

    // Background tasks
    private var audioPipelineTask: Task<Void, Never>?
    private var audioMuxTask: Task<Void, Never>?
    private var framePipelineTask: Task<Void, Never>?
    private var transcriptCollectorTask: Task<Void, Never>?
    private var frameCollectorTask: Task<Void, Never>?
    private var timerTask: Task<Void, Never>?
    private var checkpointTask: Task<Void, Never>?

    // Video recording (phone path in PR 1)
    private var videoRecorder: VideoRecorder?
    private var visualSampleProcessor: VisualSampleProcessor?
    private var videoRecordingFailed = false
    private var audioRouteMonitor: AudioRouteMonitor?

    // MARK: - Init

    init(appState: AppState? = nil) {
        self.appState = appState
        NSLog("[SessionRecorder] Initialized")
    }

    /// Set the AppState reference (called from view layer)
    func setAppState(_ state: AppState) {
        self.appState = state
    }

    /// Use the app-wide CaptureManager so device registration/connection state matches the UI.
    func setCaptureManager(_ manager: CaptureManager) {
        self.sharedCaptureManager = manager
    }

    // MARK: - Recording Lifecycle

    /// Start a new recording session with the user's preferred capture source.
    /// Creates fresh pipeline instances each time to avoid stale AsyncStream issues.
    func startRecording(preferredSource: CaptureSource = .phone) async throws {
        NSLog("[SessionRecorder] startRecording(preferredSource: \(preferredSource.rawValue)) called")

        let newSessionId = UUID()
        sessionId = newSessionId
        sessionStartTime = Date()

        // Reset collected data
        collectedFrames = []
        collectedSegments = []
        collectedBookmarks = []
        textBuffer = ""
        autoBookmarkCount = 0
        smartDetector.reset()

        // Fresh pipeline instances each session (AsyncStream lazy vars are one-time-use).
        // Reuse the app CaptureManager so glasses device/permission state matches the UI.
        guard let captureManager = sharedCaptureManager else {
            throw NSError(
                domain: "SessionRecorder",
                code: -2,
                userInfo: [NSLocalizedDescriptionKey: "Capture manager not configured"]
            )
        }
        captureManager.resetProvidersForNewSession()
        self.captureManager = captureManager
        audioPipeline = AudioPipeline()
        framePipeline = FramePipeline()

        videoRecorder = nil
        visualSampleProcessor = nil
        videoRecordingFailed = false

        appState?.sessionStatus = .recording
        appState?.liveTranscriptStatus = .idle
        appState?.liveTranscriptWarning = nil
        if preferredSource == .glasses {
            appState?.liveTranscriptHint = "Connecting glasses mic · live transcription on iPhone…"
        } else {
            appState?.liveTranscriptHint = "Connecting to live transcription…"
        }
        appState?.audioSourceWarning = nil

        let processor = VisualSampleProcessor()

        appState?.videoRecordingWarning = nil

        // Prepare video recording before capture starts (skipped on Simulator)
        #if !targetEnvironment(simulator)
        if NoteVConfig.Video.enabled {
            if !NoteVConfig.Video.hasSufficientDiskSpace {
                NSLog("[SessionRecorder] WARNING: Low disk space — skipping video recording")
                appState?.videoRecordingWarning =
                    "Low storage space — session video was not recorded. Transcript and frames will still be saved."
            } else {
                let videoURL = sessionStore.videoURL(for: newSessionId)
                let recorder = VideoRecorder()
                do {
                    try sessionStore.ensureSessionDirectory(for: newSessionId)
                    try recorder.startRecording(to: videoURL)
                    processor.videoRecorder = recorder
                    videoRecorder = recorder
                } catch {
                    NSLog("[SessionRecorder] WARNING: Could not start video recording: \(error.localizedDescription)")
                    videoRecordingFailed = true
                }
            }
        }
        #endif

        visualSampleProcessor = processor

        let provider = captureManager.selectProvider(preferredSource: preferredSource)
        provider.visualSampleProcessor = processor
        provider.videoRecorder = videoRecorder

        framePipeline.onSamplingIntervalChanged = { interval in
            processor.setSamplingInterval(interval)
        }

        // Audio from active provider (glasses mic via Bluetooth HFP, or phone mic).
        // Tee before capture starts so no chunks are dropped during HFP/DAT spin-up.
        let audioStream = provider.audioStream
        let frameStream = provider.frameStream
        let (sttAudioStream, muxAudioStream) = AudioStreamTee.tee(audioStream)

        audioPipeline.onLiveTranscriptStatusChange = { [weak self] status in
            self?.appState?.liveTranscriptStatus = status
            switch status {
            case .unavailable:
                self?.appState?.liveTranscriptHint = nil
                self?.appState?.liveTranscriptWarning =
                    "Live transcription unavailable — recording continues. Transcript will be recovered after class."
            case .connecting:
                if self?.appState?.liveTranscriptHint == nil {
                    self?.appState?.liveTranscriptHint = "Connecting to live transcription…"
                }
            case .streaming:
                self?.appState?.liveTranscriptHint = nil
                self?.appState?.liveTranscriptWarning = nil
            case .idle:
                break
            }
        }

        // Pre-connect Deepgram while glasses HFP + DAT stream spin up (~5–15s head start on LTE).
        if NoteVConfig.Audio.sttProvider == .deepgram {
            audioPipeline.prepareDeepgramConnection()
        }

        // Start capture with user's preferred source
        do {
            try await captureManager.startCapture(preferredSource: preferredSource)
        } catch {
            // Roll back state on failure
            NSLog("[SessionRecorder] ERROR: startCapture failed — rolling back state")
            audioPipeline?.stop()
            if let recorder = videoRecorder {
                _ = try? await recorder.finishRecording()
                videoRecorder = nil
                visualSampleProcessor = nil
            }
            appState?.sessionStatus = .error(error.localizedDescription)
            appState?.phoneStatus = .connected
            sessionStartTime = nil
            sessionId = nil
            throw error
        }

        // Only set isRecording after successful start [P2 fix]
        isRecording = true

        guard captureManager.activeProvider != nil else {
            NSLog("[SessionRecorder] ERROR: No active provider after startCapture")
            isRecording = false
            appState?.sessionStatus = .error("No capture provider available")
            throw NSError(domain: "SessionRecorder", code: -1, userInfo: [NSLocalizedDescriptionKey: "No capture provider available"])
        }

        // Update UI status based on active source
        appState?.activeCaptureSource = captureManager.activeSource
        if captureManager.activeSource == .glasses {
            appState?.glassesStatus = .active
            appState?.phoneStatus = .connected
            startGlassesAudioRouteMonitoring()
        } else {
            appState?.phoneStatus = .active
        }
        NSLog("[SessionRecorder] Active capture source: \(captureManager.activeSource.rawValue)")

        // Start pipelines as concurrent tasks (streams already tee'd above).
        audioPipelineTask = Task.detached { [audioPipeline] in
            guard let pipeline = audioPipeline else { return }
            await pipeline.startProcessing(audioStream: sttAudioStream)
        }

        if NoteVConfig.Video.enabled, visualSampleProcessor != nil {
            startAudioMuxCollector(stream: muxAudioStream, processor: processor)
        }

        framePipelineTask = Task.detached { [framePipeline] in
            guard let pipeline = framePipeline else { return }
            await pipeline.startProcessing(frameStream: frameStream)
        }

        let transcriptStream = audioPipeline.transcriptStream

        // Start collector tasks
        startTranscriptCollector(stream: transcriptStream)
        startFrameCollector()
        startTimer()
        startCheckpointTask()

        NSLog("[SessionRecorder] All pipelines started — session: \(newSessionId)")
    }

    /// Stop recording and assemble SessionData.
    func stopRecording() async -> SessionData {
        NSLog("[SessionRecorder] stopRecording() called")
        isRecording = false
        appState?.sessionStatus = .stopping
        audioRouteMonitor?.stop()
        audioRouteMonitor = nil

        // 1. Cancel timer + checkpoint
        timerTask?.cancel()
        timerTask = nil
        checkpointTask?.cancel()
        checkpointTask = nil

        // 2. Stop capture — raw streams finish, pipeline for-await loops will exit
        await captureManager.stopCapture()

        // 3. Await pipeline tasks — they drain remaining raw input and produce output.
        //    FramePipeline's for-await exits when frameStream finishes.
        //    AudioPipeline's for-await exits when audioStream finishes (all audio fed to recognition).
        await framePipelineTask?.value
        framePipelineTask = nil
        await audioPipelineTask?.value
        audioPipelineTask = nil
        await audioMuxTask?.value
        audioMuxTask = nil

        // 4. Signal recognition engine to produce final result (endAudio, not cancel)
        audioPipeline.endAudioInput()

        // 5. Wait for terminal recognition callback (with timeout fallback)
        await audioPipeline.waitForFinalResult(timeoutNanoseconds: 5_000_000_000)

        // 6. Now safe to finish output streams → collectors' for-await loops exit
        await audioPipeline.finishOutputStream()
        framePipeline.stop()

        // 7. Await collectors — they drain remaining buffered items
        await transcriptCollectorTask?.value
        await frameCollectorTask?.value
        transcriptCollectorTask = nil
        frameCollectorTask = nil
        NSLog("[SessionRecorder] Collector tasks drained")

        // 8. Drain capture + processor + recorder queues before finishing MP4
        await captureManager.flushPendingSamples()
        if let processor = visualSampleProcessor {
            await processor.flushAndWait()
        }

        // 9. Finish MP4 after pipelines, collectors, and queues have drained
        let endDate = Date()
        let duration = endDate.timeIntervalSince(sessionStartTime ?? endDate)

        var savedVideoFilename: String? = nil
        var muxedAudioSamples = 0
        if let recorder = videoRecorder {
            muxedAudioSamples = recorder.muxedAudioSampleCount
            do {
                if let videoURL = try await recorder.finishRecording() {
                    savedVideoFilename = NoteVConfig.Storage.sessionVideoFilename
                    NSLog("[SessionRecorder] Session video saved")

                    let asset = AVURLAsset(url: videoURL)
                    let containerDuration = CMTimeGetSeconds((try? await asset.load(.duration)) ?? .zero)
                    let audioDuration = (try? await SessionTranscriptExtractor.audioTrackDuration(in: asset)) ?? 0
                    let effectiveVideoDuration = SessionTranscriptExtractor.referenceDurationForValidation(
                        audioDuration: audioDuration,
                        containerVideoDuration: containerDuration
                    )
                    NSLog("[SessionRecorder] MP4 audio health — muxedSamples: \(muxedAudioSamples), audioTrack: \(String(format: "%.1f", audioDuration))s, video: \(String(format: "%.1f", effectiveVideoDuration))s, session: \(String(format: "%.1f", duration))s")

                    let minSamples = max(1, Int(duration * 2))
                    if muxedAudioSamples < minSamples || (effectiveVideoDuration > 0 && audioDuration < effectiveVideoDuration * 0.5) {
                        if appState?.videoRecordingWarning == nil {
                            appState?.videoRecordingWarning =
                                "Session audio may be incomplete — transcript recovery could be impaired."
                        }
                    }
                }
            } catch {
                videoRecordingFailed = true
                NSLog("[SessionRecorder] ERROR finishing video: \(error.localizedDescription)")
            }
            videoRecorder = nil
            visualSampleProcessor = nil
        }

        if videoRecordingFailed {
            NSLog("[SessionRecorder] WARNING: Video recording failed — transcript and frames were still saved")
            if appState?.videoRecordingWarning == nil {
                appState?.videoRecordingWarning =
                    "Session video could not be saved. Your transcript and captured frames were saved."
            }
        }

        let metadata = SessionMetadata(
            sessionId: sessionId ?? UUID(),
            startDate: sessionStartTime ?? endDate,
            endDate: endDate,
            captureSource: captureManager.activeSource,
            durationSeconds: duration,
            videoFilename: savedVideoFilename
        )

        // [P1 fix] Keep all segments with text, not just isFinal
        let meaningfulSegments = deduplicateSegments(collectedSegments)

        let session = SessionData(
            metadata: metadata,
            frames: collectedFrames,
            transcriptSegments: meaningfulSegments,
            bookmarks: collectedBookmarks
        )

        NSLog("[SessionRecorder] Session assembled — \(collectedFrames.count) frames, \(meaningfulSegments.count) segments (from \(collectedSegments.count) raw), \(collectedBookmarks.count) bookmarks, \(String(format: "%.0f", duration))s duration")

        // Save session to disk
        do {
            try sessionStore.save(session: session)
            NSLog("[SessionRecorder] Session saved to disk")
        } catch {
            NSLog("[SessionRecorder] ERROR saving session: \(error.localizedDescription)")
        }

        // Reset
        collectedFrames = []
        collectedSegments = []
        collectedBookmarks = []
        sessionStartTime = nil

        appState?.phoneStatus = .connected

        return session
    }

    // MARK: - Segment Deduplication

    /// Build a clean transcript from raw segments.
    /// AudioPipeline yields incremental text for both interim and final segments.
    /// Strategy: prefer final segments; for a given time window, drop interim segments
    /// that overlap with a nearby final segment; then deduplicate restart-boundary overlaps.
    private func deduplicateSegments(_ segments: [TranscriptSegment]) -> [TranscriptSegment] {
        let nonEmpty = segments
            .filter { !$0.text.trimmingCharacters(in: .whitespaces).isEmpty }
            .sorted { $0.startTime < $1.startTime }

        guard !nonEmpty.isEmpty else { return [] }

        // Partition into final and interim
        let finals = Set(nonEmpty.filter(\.isFinal).map(\.id))

        // Drop interim segments whose text is a substring of a nearby final segment (within 3s).
        // Only match segments with 4+ characters to avoid false positives on short common words.
        let filtered = nonEmpty.filter { seg in
            if finals.contains(seg.id) { return true }
            let trimmed = seg.text.trimmingCharacters(in: .whitespaces)
            guard trimmed.count >= 4 else { return true } // Keep short segments — too ambiguous to match
            let isRedundant = nonEmpty.contains { other in
                guard finals.contains(other.id) else { return false }
                guard abs(other.startTime - seg.startTime) < 3.0 else { return false }
                return other.text.localizedCaseInsensitiveContains(trimmed)
            }
            return !isRedundant
        }

        // Deduplicate restart-boundary overlaps: if consecutive segments have identical text, keep first
        var result: [TranscriptSegment] = []
        for seg in filtered {
            if let last = result.last, last.text == seg.text {
                continue
            }
            result.append(seg)
        }

        return result
    }

    // MARK: - Collector Tasks

    private func startAudioMuxCollector(stream: AsyncStream<AudioChunk>, processor: VisualSampleProcessor) {
        audioMuxTask = Task.detached(priority: .userInitiated) {
            let sourceRate = Double(NoteVConfig.Audio.sampleRate)
            let muxRate = Double(NoteVConfig.Audio.muxSampleRate)
            for await chunk in stream {
                let muxData = AudioResampler.resamplePCM16Mono(chunk.data, from: sourceRate, to: muxRate) ?? chunk.data
                processor.processAudioPCM(
                    data: muxData,
                    sessionRelativeTime: chunk.timestamp,
                    sampleRate: muxRate
                )
            }
        }
    }

    private func startTranscriptCollector(stream: AsyncStream<TranscriptSegment>) {
        transcriptCollectorTask = Task { [weak self] in
            for await segment in stream {
                guard let self = self else { break }
                self.collectedSegments.append(segment)

                // Update UI on main actor
                self.appState?.transcriptSegments.append(segment)
                if self.appState?.liveTranscriptStatus != .unavailable {
                    self.appState?.liveTranscriptStatus = .streaming
                    self.appState?.liveTranscriptHint = nil
                    self.appState?.liveTranscriptWarning = nil
                }

                // Smart bookmark detection — only on final segments
                if NoteVConfig.SmartBookmark.enabled && segment.isFinal {
                    self.checkSmartBookmark(segment: segment)
                }
            }
        }
    }

    // MARK: - Smart Bookmark Detection

    private func checkSmartBookmark(segment: TranscriptSegment) {
        guard autoBookmarkCount < NoteVConfig.SmartBookmark.maxAutoBookmarksPerSession else { return }

        // Update rolling text buffer (keep last N seconds of text)
        let bufferWindow = NoteVConfig.SmartBookmark.rollingBufferSeconds
        let recentSegments = collectedSegments.filter { seg in
            seg.isFinal && (segment.startTime - seg.startTime) <= bufferWindow
        }
        textBuffer = recentSegments.map(\.text).joined(separator: " ")

        let sessionTime = segment.startTime
        guard let result = smartDetector.detect(
            text: segment.text,
            fullBuffer: textBuffer,
            sessionTime: sessionTime
        ) else { return }

        // Create auto bookmark
        let context = (appState?.transcriptSegments.suffix(5).map(\.text).joined(separator: " ")) ?? ""
        let bookmark = Bookmark(
            timestamp: sessionTime,
            surroundingTranscript: context,
            label: result.label,
            source: .auto,
            confidence: result.confidence,
            triggerPhrase: result.triggerPhrase,
            detectionTier: result.tier
        )
        collectedBookmarks.append(bookmark)
        autoBookmarkCount += 1

        // Update UI (don't increment bookmarkCount — that's for manual only, avoids double toast)
        appState?.bookmarkTimestamps.append(sessionTime)
        appState?.autoBookmarkCount = autoBookmarkCount
        appState?.latestAutoBookmarkPhrase = result.triggerPhrase

        NSLog("[SessionRecorder] Smart bookmark #\(autoBookmarkCount) at \(String(format: "%.1f", sessionTime))s — tier \(result.tier), confidence \(String(format: "%.2f", result.confidence)), phrase: '\(result.triggerPhrase)'")
    }

    private func startFrameCollector() {
        let sigStream = framePipeline.significantFrameStream
        let store = self.imageStore

        frameCollectorTask = Task.detached { [weak self] in
            for await frame in sigStream {
                // Save image to disk off main thread
                if let imageData = frame.imageData, let sid = await self?.sessionId {
                    do {
                        try store.saveImage(imageData, filename: frame.imageFilename, sessionId: sid)
                    } catch {
                        NSLog("[SessionRecorder] ERROR saving frame image: \(error.localizedDescription)")
                    }
                }

                // Update collected data and UI on main actor
                await MainActor.run { [weak self] in
                    guard let self = self else { return }
                    var storedFrame = frame
                    storedFrame.imageData = nil
                    self.collectedFrames.append(storedFrame)
                    self.appState?.frameCount = self.collectedFrames.count
                    self.appState?.latestFrameData = frame.imageData
                }
            }
        }
    }

    // MARK: - Manual Bookmark

    /// Trigger a bookmark manually (from UI button).
    func triggerManualBookmark() async {
        guard isRecording, let sid = sessionId else { return }
        let timestamp = Date().timeIntervalSince(sessionStartTime ?? Date())

        // Build context from recent transcript segments
        let context = (appState?.transcriptSegments.suffix(5).map(\.text).joined(separator: " ")) ?? ""

        var bookmark = Bookmark(
            timestamp: timestamp,
            surroundingTranscript: context,
            source: .manual
        )

        let filename = "bookmark_\(collectedBookmarks.count + 1).jpg"

        // Capture high-res photo
        if let provider = captureManager.activeProvider {
            do {
                let photoData = try await provider.capturePhoto()
                try imageStore.saveImage(photoData, filename: filename, sessionId: sid)
                bookmark = Bookmark(
                    timestamp: timestamp,
                    frameFilename: filename,
                    surroundingTranscript: context,
                    source: .manual
                )

                // Only create .bookmark TimestampedFrame when photo was actually saved
                var bookmarkFrame = TimestampedFrame(
                    timestamp: timestamp,
                    trigger: .bookmark,
                    changeScore: 1.0,
                    imageFilename: filename
                )
                bookmarkFrame.imageData = nil
                collectedFrames.append(bookmarkFrame)
                appState?.frameCount = collectedFrames.count
                appState?.latestFrameData = photoData
            } catch {
                NSLog("[SessionRecorder] ERROR capturing manual bookmark photo: \(error.localizedDescription)")
            }
        }

        // Publish timestamp for live UI bookmark markers (even without photo)
        appState?.bookmarkTimestamps.append(timestamp)

        collectedBookmarks.append(bookmark)
        appState?.bookmarkCount = collectedBookmarks.count
        let hasFrame = bookmark.frameFilename != nil
        NSLog("[SessionRecorder] Manual bookmark #\(collectedBookmarks.count) at \(String(format: "%.1f", timestamp))s\(hasFrame ? " — .bookmark frame added" : " — no photo, frame skipped")")
    }

    /// Flush in-flight video samples to disk queues (call on background/disconnect).
    func flushRecordingPipeline() async {
        guard isRecording else { return }
        await captureManager?.flushPendingSamples()
        await visualSampleProcessor?.flushAndWait()
        NSLog("[SessionRecorder] Recording pipeline flushed")
    }

    /// Persist partial session state while recording so transcript/frames survive crashes.
    func saveRecordingCheckpoint() {
        guard isRecording,
              let sessionId,
              let sessionStartTime else { return }

        let duration = Date().timeIntervalSince(sessionStartTime)
        let existingCourse = try? sessionStore.load(sessionId: sessionId)

        let checkpoint = RecordingCheckpointBuilder.makeSessionData(
            sessionId: sessionId,
            sessionStartTime: sessionStartTime,
            duration: duration,
            captureSource: captureManager?.activeSource ?? .phone,
            frames: collectedFrames,
            transcriptSegments: deduplicateSegments(collectedSegments),
            bookmarks: collectedBookmarks,
            existingCourse: existingCourse
        )

        do {
            try sessionStore.save(session: checkpoint)
            NSLog("[SessionRecorder] Checkpoint saved — \(checkpoint.frames.count) frames, \(checkpoint.transcriptSegments.count) segments, \(String(format: "%.0f", duration))s")
        } catch {
            NSLog("[SessionRecorder] ERROR saving checkpoint: \(error.localizedDescription)")
        }
    }

    private func startCheckpointTask() {
        checkpointTask?.cancel()
        checkpointTask = Task { [weak self] in
            let interval = UInt64(NoteVConfig.LongSession.recordingCheckpointIntervalSeconds * 1_000_000_000)
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: interval)
                guard !Task.isCancelled else { break }
                await BackgroundTaskCoordinator.run(named: "NoteV.RecordingCheckpoint") {
                    await self?.flushRecordingPipeline()
                    await MainActor.run {
                        self?.saveRecordingCheckpoint()
                    }
                }
            }
        }
    }

    private func startTimer() {
        timerTask = Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 1_000_000_000) // 1 second
                guard let self = self, let start = self.sessionStartTime else { break }

                let elapsed = Date().timeIntervalSince(start)
                self.appState?.elapsedTime = elapsed
            }
        }
    }

    private func startGlassesAudioRouteMonitoring() {
        let monitor = AudioRouteMonitor()
        monitor.onGlassesMicLost = { [weak self] in
            self?.appState?.audioSourceWarning =
                "Glasses mic disconnected — audio may be coming from your iPhone. Check that glasses are worn and connected in Meta AI."
            Task { @MainActor [weak self] in
                await BackgroundTaskCoordinator.run(named: "NoteV.RecordingBackground") {
                    await self?.flushRecordingPipeline()
                    self?.saveRecordingCheckpoint()
                }
            }
        }
        monitor.startMonitoringGlassesHFP()
        audioRouteMonitor = monitor
    }

    // MARK: - Test seam

    /// Arms in-memory recording state so `saveRecordingCheckpoint()` can run without capture hardware.
    internal func armRecordingCheckpointState(
        sessionId: UUID,
        sessionStartTime: Date = Date().addingTimeInterval(-30),
        frames: [TimestampedFrame] = [],
        transcriptSegments: [TranscriptSegment] = [],
        bookmarks: [Bookmark] = []
    ) {
        self.sessionId = sessionId
        self.sessionStartTime = sessionStartTime
        isRecording = true
        collectedFrames = frames
        collectedSegments = transcriptSegments
        collectedBookmarks = bookmarks
    }
}
