import Foundation
import MWDATCore
import MWDATCamera
@preconcurrency import AVFoundation
import UIKit

// MARK: - GlassesCaptureProvider

/// Captures frames and audio from Meta Ray-Ban smart glasses via the DAT SDK.
/// Video: StreamSession → VisualSampleProcessor → MP4 + throttled JPEG frames.
/// Audio: Glasses mic via Bluetooth HFP → single 16 kHz PCM stream (Meta has no DAT audio API).
///
/// @MainActor because StreamSession and its publishers are MainActor-isolated.
@MainActor
final class GlassesCaptureProvider: CaptureProvider {

    // MARK: - Properties

    private let wearables: WearablesInterface
    private let deviceSelector: AutoDeviceSelector
    private var streamSession: StreamSession

    // DAT SDK listener tokens — MUST retain, nil = subscription canceled
    private var stateListenerToken: AnyListenerToken?
    private var videoFrameListenerToken: AnyListenerToken?
    private var errorListenerToken: AnyListenerToken?
    private var photoDataListenerToken: AnyListenerToken?
    private var deviceMonitorTask: Task<Void, Never>?
    private var pairedDevicesTask: Task<Void, Never>?
    private var pairedDeviceCount = 0

    // Audio (glasses mic via Bluetooth HFP, not DAT SDK)
    private let audioEngine = AVAudioEngine()

    // Session state
    private var sessionStartTime: Date?
    private var isStreaming = false
    private var streamFailedDuringStartup = false
    private var isAwaitingFirstStream = false

    // Photo capture async continuation
    private var photoContinuation: CheckedContinuation<Data, Error>?

    // AsyncStream continuations
    private var audioContinuation: AsyncStream<AudioChunk>.Continuation?

    private(set) var isAvailable: Bool = false

    var videoRecorder: VideoRecorder?
    var visualSampleProcessor: VisualSampleProcessor?

    /// Set in `startCapture()` so DAT callbacks can fan out samples without hopping to MainActor.
    private nonisolated(unsafe) var videoIngressProcessor: VisualSampleProcessor?

    var frameStream: AsyncStream<TimestampedFrame> {
        visualSampleProcessor?.frameStream ?? AsyncStream { $0.finish() }
    }

    lazy var audioStream: AsyncStream<AudioChunk> = {
        AsyncStream { continuation in
            self.audioContinuation = continuation
        }
    }()

    // MARK: - Init

    init(wearables: WearablesInterface = Wearables.shared) {
        self.wearables = wearables
        self.deviceSelector = AutoDeviceSelector(wearables: wearables)

        let config = StreamSessionConfig(
            videoCodec: VideoCodec.raw,
            resolution: StreamingResolution.high,
            frameRate: UInt(NoteVConfig.Video.glassesStreamFrameRate)
        )
        self.streamSession = StreamSession(
            streamSessionConfig: config,
            deviceSelector: deviceSelector
        )

        NSLog("[GlassesCaptureProvider] Initialized — monitoring device availability")

        deviceMonitorTask = Task { [weak self, deviceSelector] in
            for await device in deviceSelector.activeDeviceStream() {
                self?.isAvailable = device != nil
                NSLog("[GlassesCaptureProvider] Device availability changed: \(device != nil)")
            }
        }

        pairedDevicesTask = Task { [weak self, wearables] in
            for await devices in wearables.devicesStream() {
                self?.pairedDeviceCount = devices.count
            }
        }

        attachListeners()
    }

    deinit {
        deviceMonitorTask?.cancel()
        pairedDevicesTask?.cancel()
    }

    // MARK: - Listeners

    private func attachListeners() {
        stateListenerToken = streamSession.statePublisher.listen { [weak self] state in
            Task { @MainActor [weak self] in
                guard let self else { return }
                switch state {
                case .streaming:
                    self.isStreaming = true
                    self.streamFailedDuringStartup = false
                    NSLog("[GlassesCaptureProvider] StreamSession state: streaming")
                case .stopped:
                    self.isStreaming = false
                    if self.isAwaitingFirstStream {
                        self.streamFailedDuringStartup = true
                    }
                    NSLog("[GlassesCaptureProvider] StreamSession state: stopped")
                case .waitingForDevice:
                    NSLog("[GlassesCaptureProvider] StreamSession state: waitingForDevice")
                case .starting:
                    NSLog("[GlassesCaptureProvider] StreamSession state: starting")
                case .stopping:
                    NSLog("[GlassesCaptureProvider] StreamSession state: stopping")
                case .paused:
                    NSLog("[GlassesCaptureProvider] StreamSession state: paused")
                }
            }
        }

        videoFrameListenerToken = streamSession.videoFramePublisher.listen { [weak self] videoFrame in
            self?.videoIngressProcessor?.processVideoSample(videoFrame.sampleBuffer)
        }

        errorListenerToken = streamSession.errorPublisher.listen { [weak self] error in
            Task { @MainActor [weak self] in
                guard let self else { return }
                if !self.isStreaming {
                    if case .deviceNotConnected = error { return }
                    if case .deviceNotFound = error { return }
                }
                self.streamFailedDuringStartup = true
                NSLog("[GlassesCaptureProvider] StreamSession error: \(error)")
            }
        }

        photoDataListenerToken = streamSession.photoDataPublisher.listen { [weak self] photoData in
            Task { @MainActor [weak self] in
                guard let self else { return }
                NSLog("[GlassesCaptureProvider] Photo captured — \(photoData.data.count) bytes")
                self.photoContinuation?.resume(returning: photoData.data)
                self.photoContinuation = nil
            }
        }
    }

    // MARK: - CaptureProvider

    /// Sync paired count from CaptureManager when `devicesStream` has not emitted yet on a fresh provider.
    func notePairedDeviceCount(atLeast count: Int) {
        pairedDeviceCount = max(pairedDeviceCount, count)
    }

    /// Waits for an active glasses device, then requests Meta AI camera permission if needed.
    func ensureCameraPermission() async throws {
        try await waitForConnectedDevice(timeoutSeconds: 15)
        try await requestCameraPermissionWithRetry()
    }

    /// Start capture: configure glasses HFP mic first, then DAT video stream (Meta doc order).
    func startCapture() async throws {
        NSLog("[GlassesCaptureProvider] startCapture() called")

        _ = self.audioStream
        _ = self.frameStream

        guard visualSampleProcessor != nil else {
            throw Self.makeError(
                code: -7,
                message: "VisualSampleProcessor must be set before startCapture()"
            )
        }

        visualSampleProcessor?.reset()
        videoIngressProcessor = visualSampleProcessor

        try await ensureCameraPermission()

        streamFailedDuringStartup = false
        isStreaming = false
        isAwaitingFirstStream = true

        // Meta DAT: HFP must be configured before starting the camera stream.
        sessionStartTime = Date()

        do {
            try configureAudioEngine()
            try audioEngine.start()
            let hfpReady = await GlassesHFPRoute.waitForActive()
            guard hfpReady else {
                audioEngine.stop()
                audioEngine.inputNode.removeTap(onBus: 0)
                throw GlassesHFPRoute.missingRouteError
            }
            if !GlassesHFPRoute.isLikelyGlassesInput() {
                audioEngine.stop()
                audioEngine.inputNode.removeTap(onBus: 0)
                sessionStartTime = nil
                throw GlassesHFPRoute.wrongHFPDeviceError
            }
            NSLog("[GlassesCaptureProvider] HFP route active — starting DAT video stream")
        } catch {
            audioEngine.stop()
            audioEngine.inputNode.removeTap(onBus: 0)
            sessionStartTime = nil
            NSLog("[GlassesCaptureProvider] ERROR starting glasses mic (HFP): \(error.localizedDescription)")
            throw error
        }

        await streamSession.start()
        NSLog("[GlassesCaptureProvider] StreamSession started")

        do {
            try await waitForVideoStreaming(timeoutSeconds: 15)
        } catch {
            isAwaitingFirstStream = false
            await streamSession.stop()
            audioEngine.stop()
            audioEngine.inputNode.removeTap(onBus: 0)
            sessionStartTime = nil
            throw error
        }
        isAwaitingFirstStream = false
        NSLog("[GlassesCaptureProvider] AVAudioEngine + StreamSession streaming")
    }

    func stopCapture() async {
        NSLog("[GlassesCaptureProvider] stopCapture() called")
        isAwaitingFirstStream = false

        await streamSession.stop()

        audioEngine.stop()
        audioEngine.inputNode.removeTap(onBus: 0)

        visualSampleProcessor?.finishFrames()
        videoIngressProcessor = nil
        audioContinuation?.finish()

        photoContinuation?.resume(throwing: NSError(domain: "GlassesCaptureProvider", code: -4,
                                                     userInfo: [NSLocalizedDescriptionKey: "Capture stopped during photo"]))
        photoContinuation = nil

        sessionStartTime = nil
        NSLog("[GlassesCaptureProvider] Capture stopped")
    }

    func flushPendingSamples() async {
        await visualSampleProcessor?.flushAndWait()
    }

    func capturePhoto() async throws -> Data {
        NSLog("[GlassesCaptureProvider] capturePhoto() called")

        guard photoContinuation == nil else {
            throw NSError(domain: "GlassesCaptureProvider", code: -2,
                          userInfo: [NSLocalizedDescriptionKey: "Photo capture already in progress"])
        }

        return try await withCheckedThrowingContinuation { continuation in
            self.photoContinuation = continuation
            _ = streamSession.capturePhoto(format: .jpeg)
        }
    }

    // MARK: - Audio Configuration

    private func configureAudioEngine() throws {
        let audioSession = AVAudioSession.sharedInstance()
        try audioSession.setCategory(.playAndRecord, mode: .videoChat, options: [.defaultToSpeaker, .allowBluetoothHFP])
        try audioSession.setActive(true)
        try GlassesHFPRoute.configurePreferredInput(on: audioSession)

        let inputNode = audioEngine.inputNode
        let hardwareFormat = inputNode.inputFormat(forBus: 0)

        NSLog("[GlassesCaptureProvider] Audio hardware format: \(Int(hardwareFormat.sampleRate))Hz, \(hardwareFormat.channelCount)ch")

        guard let pcmFormat = AVAudioFormat(
            commonFormat: .pcmFormatInt16,
            sampleRate: Double(NoteVConfig.Audio.sampleRate),
            channels: AVAudioChannelCount(NoteVConfig.Audio.channels),
            interleaved: true
        ) else {
            throw NSError(domain: "GlassesCaptureProvider", code: -5,
                          userInfo: [NSLocalizedDescriptionKey: "Could not create PCM audio format"])
        }

        guard let converter = AVAudioConverter(from: hardwareFormat, to: pcmFormat) else {
            throw NSError(domain: "GlassesCaptureProvider", code: -6,
                          userInfo: [NSLocalizedDescriptionKey: "Could not create audio converter"])
        }

        let audioCont = self.audioContinuation

        inputNode.installTap(onBus: 0, bufferSize: 1024, format: hardwareFormat) { [weak self] buffer, _ in
            let timestamp: TimeInterval
            if let start = self?.sessionStartTime {
                timestamp = Date().timeIntervalSince(start)
            } else {
                timestamp = 0
            }

            guard let pcmData = Self.convertBuffer(buffer, to: pcmFormat, using: converter) else { return }
            let duration = Double(pcmData.count / 2) / pcmFormat.sampleRate
            audioCont?.yield(AudioChunk(timestamp: timestamp, data: pcmData, duration: duration))
        }

        NSLog("[GlassesCaptureProvider] Audio engine configured — \(Int(hardwareFormat.sampleRate))Hz → \(NoteVConfig.Audio.sampleRate)Hz mono PCM")
    }

    private static func convertBuffer(
        _ buffer: AVAudioPCMBuffer,
        to targetFormat: AVAudioFormat,
        using converter: AVAudioConverter
    ) -> Data? {
        let frameCount = AVAudioFrameCount(
            Double(buffer.frameLength) * targetFormat.sampleRate / buffer.format.sampleRate
        )
        guard let convertedBuffer = AVAudioPCMBuffer(pcmFormat: targetFormat, frameCapacity: frameCount) else {
            return nil
        }

        var error: NSError?
        let status = converter.convert(to: convertedBuffer, error: &error) { _, outStatus in
            outStatus.pointee = .haveData
            return buffer
        }

        guard status != .error, error == nil,
              let channelData = convertedBuffer.int16ChannelData else {
            if let error {
                NSLog("[GlassesCaptureProvider] Audio conversion error: \(error.localizedDescription)")
            }
            return nil
        }

        let byteCount = Int(convertedBuffer.frameLength) * MemoryLayout<Int16>.size
        return Data(bytes: channelData[0], count: byteCount)
    }

    private func waitForVideoStreaming(timeoutSeconds: TimeInterval) async throws {
        let deadline = Date().addingTimeInterval(timeoutSeconds)
        while Date() < deadline {
            if isStreaming { return }
            if streamFailedDuringStartup {
                throw NSError(
                    domain: "GlassesCaptureProvider",
                    code: -9,
                    userInfo: [NSLocalizedDescriptionKey:
                        "Glasses video stream failed. Put the glasses on, confirm they are connected in Meta AI, and try again."]
                )
            }
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        throw Self.makeError(
            code: -10,
            message: "Glasses video stream timed out. Ensure glasses are worn, awake, and connected via Meta AI."
        )
    }

    // MARK: - Permissions

    private func waitForConnectedDevice(timeoutSeconds: TimeInterval) async throws {
        if isAvailable { return }

        let deadline = Date().addingTimeInterval(timeoutSeconds)
        while Date() < deadline {
            if isAvailable { return }
            try await Task.sleep(nanoseconds: 200_000_000)
        }

        // UI uses wearables.devicesStream (paired/connected). AutoDeviceSelector can lag behind.
        if pairedDeviceCount > 0 {
            NSLog("[GlassesCaptureProvider] \(pairedDeviceCount) paired device(s) — proceeding to Meta camera permission")
            return
        }

        throw Self.makeError(
            code: -11,
            message: "Glasses aren't ready yet. Put them on, wake them in Meta AI, wait for the connection indicator, then try again."
        )
    }

    private func requestCameraPermissionWithRetry(maxAttempts: Int = 3) async throws {
        for attempt in 1...maxAttempts {
            do {
                let status = try await wearables.checkPermissionStatus(.camera)
                if status == .granted { return }

                NSLog("[GlassesCaptureProvider] Requesting camera permission via Meta AI (attempt \(attempt))")
                let requestStatus = try await wearables.requestPermission(.camera)
                if requestStatus == .granted { return }

                throw Self.makeError(
                    code: -3,
                    message: "Camera access denied on glasses. Open Meta AI → App Connections → NoteV and allow camera access."
                )
            } catch let error as PermissionError {
                NSLog("[GlassesCaptureProvider] Permission error (attempt \(attempt)): \(error.description)")
                if attempt < maxAttempts, error == .noDeviceWithConnection || error == .noDevice {
                    try await Task.sleep(nanoseconds: 800_000_000)
                    try await waitForConnectedDevice(timeoutSeconds: 5)
                    continue
                }
                throw Self.makeError(code: Int(error.rawValue), message: Self.userMessage(for: error))
            } catch {
                NSLog("[GlassesCaptureProvider] Permission error: \(error.localizedDescription)")
                throw error
            }
        }
    }

    static func userMessage(for error: PermissionError) -> String {
        switch error {
        case .noDevice:
            return "No glasses found. Pair your glasses in Meta AI and keep them nearby."
        case .noDeviceWithConnection:
            return "Glasses aren't connected. Open Meta AI, confirm they show as connected, then try again."
        case .metaAINotInstalled:
            return "Meta AI isn't installed. Install Meta AI to grant camera access for your glasses."
        case .requestInProgress:
            return "A permission request is already open in Meta AI. Complete it, then return to NoteV."
        case .requestTimeout:
            return "Permission request timed out. Open Meta AI and approve camera access for NoteV."
        case .connectionError:
            return "Couldn't reach your glasses. Check Bluetooth and Meta AI connection, then try again."
        case .internalError:
            return "Unexpected permission error. Try force-quitting Meta AI and NoteV, then reconnect your glasses."
        @unknown default:
            return "Camera permission failed (\(error.description)). Open Meta AI → App Connections → NoteV."
        }
    }

    static func makeError(code: Int, message: String) -> NSError {
        NSError(domain: "GlassesCaptureProvider", code: code, userInfo: [NSLocalizedDescriptionKey: message])
    }
}
